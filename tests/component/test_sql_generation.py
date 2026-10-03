"""Tests for generated SQL: builders, strategy upserts, and upsert_rows.
"""
import database as db
import pytest
from database.connection import ConnectionWrapper
from database.cursor import Cursor
from database.exceptions import ValidationError
from database.sql import build_insert_sql, build_select_sql
from database.strategy import PostgresStrategy, get_strategy


class RecordingCursor:
    """Cursor stand-in that records statements instead of running them.
    """

    def __init__(self, rowcount=None):
        self.calls = []
        self._rowcount = rowcount

    def executemany(self, operation, seq_of_parameters, batch_size=500):
        params = [list(p) for p in seq_of_parameters]
        self.calls.append((operation, params, batch_size))
        if self._rowcount is None:
            return len(params)
        return self._rowcount


class StubConnection:
    """ConnectionWrapper stand-in: real SQL building, stubbed schema and I/O.
    """

    filter_table_columns = ConnectionWrapper.filter_table_columns
    insert_row = ConnectionWrapper.insert_row
    insert_rows = ConnectionWrapper.insert_rows
    update_row = ConnectionWrapper.update_row
    upsert_rows = ConnectionWrapper.upsert_rows
    _reject_if_readonly = ConnectionWrapper._reject_if_readonly

    def __init__(
            self, dialect='postgresql', columns=(), primary_keys=(),
            rowcount=None, readonly=False):
        self.dialect = dialect
        self.readonly = readonly
        self.columns = list(columns)
        self.primary_keys = list(primary_keys)
        self.recorder = RecordingCursor(rowcount)
        self.executed = []
        self.sequence_resets = []

    def get_table_columns(self, table, bypass_cache=False):
        return list(self.columns)

    def get_table_primary_keys(self, table, bypass_cache=False):
        return list(self.primary_keys)

    def cursor(self):
        return self.recorder

    def execute(self, sql, *args):
        self.executed.append((sql, args))
        return 1

    def reset_table_sequence(self, table, identity=None):
        self.sequence_resets.append(table)


@pytest.fixture
def upsert_conn():
    """In-memory SQLite connection with three tables covering key shapes.
    """
    inventory_sql = """
create table inventory (
    sku text not null,
    warehouse text not null,
    qty integer,
    note text,
    primary key (sku, warehouse)
)
"""
    combo_sql = """
create table combo (
    id integer primary key,
    name text,
    region text,
    val integer,
    unique (name, region)
)
"""
    cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    db.execute(cn, inventory_sql)
    db.execute(cn, combo_sql)
    db.execute(cn, 'create table eventlog (a text, b text)')
    yield cn
    cn.close()


@pytest.fixture
def executed_statements(monkeypatch):
    """Record every (sql, params, batch_size) reaching Cursor.executemany.
    """
    calls = []
    original = Cursor.executemany

    def record(self, operation, seq_of_parameters, batch_size=500, **kwargs):
        params = [list(p) for p in seq_of_parameters]
        calls.append((operation, params, batch_size))
        return original(self, operation, seq_of_parameters, batch_size, **kwargs)

    monkeypatch.setattr(Cursor, 'executemany', record)
    return calls


@pytest.fixture
def constraint_lookups(monkeypatch):
    """Stub PostgreSQL constraint resolution and record what was asked for.
    """
    lookups = []

    def fake_definition(self, cn, table, constraint_name):
        lookups.append((table, constraint_name))
        return '(lower("name")) WHERE "email" IS NOT NULL'

    monkeypatch.setattr(
        PostgresStrategy, 'get_constraint_definition', fake_definition)
    return lookups


@pytest.mark.parametrize('dialect', ['postgresql', 'sqlite'])
def test_select_without_columns_uses_star(dialect):
    """Verify build_select_sql() emits 'select *' only when columns is empty.

    Mutation: swapping the `if columns:` branches in build_select_sql.
    Oracle: hand-written literals for both the empty and non-empty case.
    """
    assert build_select_sql('users', dialect) == 'select * from "users"'
    assert build_select_sql('users', dialect, columns=[]) == 'select * from "users"'
    assert (build_select_sql('users', dialect, columns=['id'])
            == 'select "id" from "users"')


@pytest.mark.parametrize('dialect', ['postgresql', 'sqlite'])
def test_select_quotes_each_identifier(dialect):
    """Verify every column and the table are quoted separately, in order.

    Mutation: quoting the column list as one identifier in build_select_sql.
    Oracle: hand-written literal with a dotted table and a spaced column.
    """
    sql = build_select_sql('public.users', dialect, columns=['id', 'full name'])
    assert sql == 'select "id", "full name" from "public"."users"'


def test_select_limit_zero_is_emitted():
    """Verify 'limit 0' survives while limit=None emits no limit clause.

    Mutation: `if limit is not None:` weakened to `if limit:`.
    Oracle: boundary pair straddling the falsy/None distinction.
    """
    assert (build_select_sql('users', 'postgresql', limit=0)
            == 'select * from "users" limit 0')
    assert (build_select_sql('users', 'postgresql', limit=10)
            == 'select * from "users" limit 10')
    assert build_select_sql('users', 'postgresql') == 'select * from "users"'


@pytest.mark.parametrize(('dialect', 'expected'), [
    ('postgresql', 'insert into "users" ("id", "name") values (%s, %s)'),
    ('sqlite', 'insert into "users" ("id", "name") values (?, ?)'),
    ], ids=['postgresql', 'sqlite'])
def test_insert_placeholder_marker_per_dialect(dialect, expected):
    """Verify the placeholder marker follows the dialect, one per column.

    Mutation: flipping the ternary in make_placeholders.
    Oracle: hand-written literal per dialect.
    """
    assert build_insert_sql(
        dialect=dialect, table='users',
        columns=['id', 'name']) == expected


class TestSQLGeneration:
    """Full-statement pins for the sql.py builders."""

    @pytest.mark.parametrize('dialect', ['postgresql', 'sqlite'])
    def test_build_select_sql(self, dialect):
        """Verify all four optional clauses render in SQL order.

        Mutation: swapping the order of two clause appends in build_select_sql.
        Oracle: hand-written literal for the whole statement.
        """
        sql = build_select_sql(
            'users', dialect, columns=['id', 'name'],
            where='active = true', order_by='"name"', limit=10)
        expected = (
            'select "id", "name" from "users" '
            'where active = true order by "name" limit 10'
            )
        assert sql == expected

    def test_build_insert_sql(self):
        """Verify a dotted table splits and an embedded quote is doubled.

        Mutation: one wrap in place of quote_identifier's per-segment quoting.
        Oracle: hand-written literal following the SQL escaping rule.
        """
        sql = build_insert_sql(
            dialect='sqlite', table='main.users',
            columns=['id', 'na"me'])
        assert sql == 'insert into "main"."users" ("id", "na""me") values (?, ?)'


@pytest.mark.parametrize(('dialect', 'expected'), [
    ('postgresql',
     ('insert into "mytable" ("a", "b", "c") values (%s, %s, %s) '
      'on conflict ("a", "b") do nothing')),
    ('sqlite',
     ('insert into "mytable" ("a", "b", "c") values (?, ?, ?) '
      'on conflict ("a", "b") do nothing')),
], ids=['postgresql', 'sqlite'])
def test_upsert_sql_do_nothing_without_update_columns(dialect, expected):
    """Verify no update columns yields do nothing on the key column list.

    Mutation: the do-nothing early return deleted from either build_upsert_sql.
    Oracle: hand-written literal per dialect.
    """
    sql = get_strategy(dialect).build_upsert_sql(
        table='mytable',
        columns=['a', 'b', 'c'],
        key_columns=['a', 'b'])
    assert sql == expected


@pytest.mark.parametrize(('dialect', 'expected'), [
    ('postgresql',
     ('insert into "mytable" ("a", "b", "c") values (%s, %s, %s) '
      'on conflict ("a") do update set "c" = excluded."c", "b" = excluded."b"')),
    ('sqlite',
     ('insert into "mytable" ("a", "b", "c") values (?, ?, ?) '
      'on conflict ("a") do update set "c" = excluded."c", "b" = excluded."b"')),
], ids=['postgresql', 'sqlite'])
def test_upsert_sql_do_update_set_exact(dialect, expected):
    """Verify always-updated columns render as col = excluded.col, in order.

    Mutation: conflict target built from quoted_columns over quoted_keys.
    Oracle: hand-written literal per dialect.
    """
    sql = get_strategy(dialect).build_upsert_sql(
        table='mytable',
        columns=['a', 'b', 'c'],
        key_columns=['a'],
        update_cols_always=['c', 'b'])
    assert sql == expected


def test_upsert_sql_coalesce_prefers_existing_row():
    """Verify the ifnull column coalesces the stored value ahead of excluded.

    Mutation: the coalesce arguments swapped in _build_update_exprs.
    Oracle: hand-written literal; argument order decides the semantics.
    """
    sql = get_strategy('postgresql').build_upsert_sql(
        table='mytable',
        columns=['a', 'b'],
        key_columns=['a'],
        update_cols_ifnull=['b'])
    expected = (
        'insert into "mytable" ("a", "b") values (%s, %s) '
        'on conflict ("a") '
        'do update set "b" = coalesce("mytable"."b", excluded."b")'
        )
    assert sql == expected


def test_upsert_sql_always_columns_precede_ifnull_columns():
    """Verify the set list runs always-columns first, then ifnull-columns.

    Mutation: the ifnull block appended before the always block.
    Oracle: hand-written literal with one column of each kind.
    """
    sql = get_strategy('postgresql').build_upsert_sql(
        table='t',
        columns=['k', 'x', 'y'],
        key_columns=['k'],
        update_cols_always=['x'],
        update_cols_ifnull=['y'])
    expected = (
        'insert into "t" ("k", "x", "y") values (%s, %s, %s) '
        'on conflict ("k") '
        'do update set "x" = excluded."x", '
        '"y" = coalesce("t"."y", excluded."y")'
        )
    assert sql == expected


def test_upsert_sql_constraint_expression_replaces_column_target():
    """Verify a constraint expression replaces the key column target verbatim.

    Mutation: dropping the `if constraint_expr:` branch for PostgreSQL.
    Oracle: hand-written literal with a partial-index where clause.
    """
    sql = get_strategy('postgresql').build_upsert_sql(
        table='users',
        columns=['id', 'name'],
        key_columns=['id'],
        constraint_expr='(lower("name")) WHERE "name" IS NOT NULL',
        update_cols_always=['name'])
    expected = (
        'insert into "users" ("id", "name") values (%s, %s) '
        'on conflict (lower("name")) WHERE "name" IS NOT NULL '
        'do update set "name" = excluded."name"'
        )
    assert sql == expected


def test_upsert_sql_sqlite_ignores_constraint_expression():
    """Verify SQLite builds the conflict target from key columns regardless.

    Mutation: constraint_expr honored in the SQLite build_upsert_sql.
    Oracle: hand-written literal; the PostgreSQL call differs.
    """
    kwargs = {
        'table': 't',
        'columns': ['k', 'v'],
        'key_columns': ['k'],
        'constraint_expr': 'on constraint uq_t',
        'update_cols_always': ['v'],
        }
    sqlite_sql = get_strategy('sqlite').build_upsert_sql(**kwargs)
    expected = (
        'insert into "t" ("k", "v") values (?, ?) '
        'on conflict ("k") do update set "v" = excluded."v"'
        )
    assert sqlite_sql == expected
    assert 'uq_t' in get_strategy('postgresql').build_upsert_sql(**kwargs)


def test_upsert_sql_quotes_schema_qualified_table():
    """Verify a dotted table is split in both the insert and the coalesce.

    Mutation: the table quoted as one identifier in _build_update_exprs.
    Oracle: hand-written literal with the schema split applied twice.
    """
    sql = get_strategy('postgresql').build_upsert_sql(
        table='public.t',
        columns=['a', 'b'],
        key_columns=['a'],
        update_cols_ifnull=['b'])
    expected = (
        'insert into "public"."t" ("a", "b") values (%s, %s) '
        'on conflict ("a") '
        'do update set "b" = coalesce("public"."t"."b", excluded."b")'
        )
    assert sql == expected


def test_upsert_rows_uses_table_column_order(upsert_conn, executed_statements):
    """Verify insert columns and parameters follow table order.

    Mutation: columns sorted, or taken in the caller's key order.
    Oracle: a row dict in neither table nor alphabetical order.
    """
    rows = ({'note': 'n', 'qty': 5, 'warehouse': 'W1', 'sku': 'A'},)
    upsert_conn.upsert_rows('inventory', rows, update_cols_always=['qty'])

    sql, params, _ = executed_statements[-1]
    expected = (
        'insert into "inventory" ("sku", "warehouse", "qty", "note") '
        'values (?, ?, ?, ?) '
        'on conflict ("sku", "warehouse") do update set "qty" = excluded."qty"'
        )
    assert sql == expected
    assert params == [['A', 'W1', 5, 'n']]


def test_upsert_rows_corrects_case_and_drops_unknown_columns(
        upsert_conn, executed_statements):
    """Verify column names are case-corrected and unknown keys are dropped.

    Mutation: the case_map lookup dropped in filter_table_columns.
    Oracle: hand-written literal naming the schema columns only.
    """
    rows = ({'SKU': 'A', 'Warehouse': 'W1', 'QTY': 5, 'bogus': 1},)
    upsert_conn.upsert_rows('inventory', rows, update_cols_always=['QTY'])

    sql, params, _ = executed_statements[-1]
    expected = (
        'insert into "inventory" ("sku", "warehouse", "qty") '
        'values (?, ?, ?) '
        'on conflict ("sku", "warehouse") do update set "qty" = excluded."qty"'
        )
    assert sql == expected
    assert params == [['A', 'W1', 5]]


def test_upsert_rows_omits_key_columns_from_update_set(
        upsert_conn, executed_statements):
    """Verify a key column named in update_cols_always is filtered out.

    Mutation: updatable_lower built without subtracting key_cols_lower.
    Oracle: hand-written literal whose set list holds only the non-key column.
    """
    rows = ({'sku': 'A', 'warehouse': 'W1', 'qty': 5},)
    upsert_conn.upsert_rows(
        'inventory', rows,
        update_cols_always=['sku', 'qty', 'warehouse'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'insert into "inventory" ("sku", "warehouse", "qty") '
        'values (?, ?, ?) '
        'on conflict ("sku", "warehouse") do update set "qty" = excluded."qty"'
        )
    assert sql == expected


def test_upsert_rows_ifnull_skips_columns_already_always_updated(
        upsert_conn, executed_statements):
    """Verify a column in both update lists is set once, unconditionally.

    Mutation: update_cols_ifnull filtered on updatable_lower over ifnull_lower.
    Oracle: hand-written literal with one assignment per column.
    """
    rows = ({'sku': 'A', 'warehouse': 'W1', 'qty': 5, 'note': 'n'},)
    upsert_conn.upsert_rows(
        'inventory', rows,
        update_cols_always=['qty'],
        update_cols_ifnull=['qty', 'note'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'insert into "inventory" ("sku", "warehouse", "qty", "note") '
        'values (?, ?, ?, ?) '
        'on conflict ("sku", "warehouse") '
        'do update set "qty" = excluded."qty", '
        '"note" = coalesce("inventory"."note", excluded."note")'
        )
    assert sql == expected


def test_upsert_rows_ifnull_excludes_key_columns(
        upsert_conn, executed_statements):
    """Verify key columns listed in update_cols_ifnull are filtered out.

    Mutation: updatable_lower built without subtracting key_cols_lower.
    Oracle: hand-written literal whose set list holds only the non-key column.
    """
    rows = ({'sku': 'A', 'warehouse': 'W1', 'qty': 1, 'note': 'n'},)
    upsert_conn.upsert_rows('inventory', rows, update_cols_ifnull=['sku', 'note'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'insert into "inventory" ("sku", "warehouse", "qty", "note") '
        'values (?, ?, ?, ?) '
        'on conflict ("sku", "warehouse") '
        'do update set "note" = coalesce("inventory"."note", excluded."note")'
        )
    assert sql == expected


def test_filter_table_columns_drops_unknown_keys():
    """Verify filter_table_columns drops keys absent from the schema.

    Mutation: filter_table_columns keeping unknown keys verbatim.
    Oracle: hand-written insert literal with the two schema columns.
    """
    cn = StubConnection('postgresql', ['id', 'name'], ['id'])
    cn.insert_rows('t', ({'id': 1, 'name': 'a', 'bogus': 9},))

    sql, params, _ = cn.recorder.calls[-1]
    assert sql == 'insert into "t" ("id","name") values (%s, %s)'
    assert params == [[1, 'a']]


def test_upsert_rows_conflict_columns_override_primary_key(
        upsert_conn, executed_statements):
    """Verify conflict_columns replace the primary key and are case-corrected.

    Mutation: key_cols taken from the raw conflict_columns list.
    Oracle: hand-written literal; SQLite rejects the uncorrected name.
    """
    rows = ({'id': 1, 'name': 'x', 'region': 'y', 'val': 3},)
    upsert_conn.upsert_rows(
        'combo', rows, conflict_columns=['NAME', 'Region'],
        update_cols_always=['val', 'Name'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'insert into "combo" ("id", "name", "region", "val") '
        'values (?, ?, ?, ?) '
        'on conflict ("name", "region") do update set "val" = excluded."val"'
        )
    assert sql == expected


def test_upsert_rows_uses_unique_index_when_primary_key_absent(
        upsert_conn, executed_statements):
    """Verify SQLite falls back to a unique index when the key is not supplied.

    Mutation: `not use_primary_key` flipped in the unique-index fallback.
    Oracle: hand-written literal on the unique index; 'id' never appears.
    """
    rows = ({'name': 'x', 'region': 'y', 'val': 3},)
    upsert_conn.upsert_rows('combo', rows, update_cols_always=['val'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'insert into "combo" ("name", "region", "val") values (?, ?, ?) '
        'on conflict ("name", "region") do update set "val" = excluded."val"'
        )
    assert sql == expected


def test_upsert_rows_requires_every_unique_column_present(
        upsert_conn, executed_statements):
    """Verify a partly supplied unique index is not used as a conflict target.

    Mutation: all() weakened to any() on the unique-column check.
    Oracle: hand-written plain insert literal; one of two index columns given.
    """
    rows = ({'name': 'x', 'val': 3},)
    upsert_conn.upsert_rows('combo', rows, update_cols_always=['val'])

    sql, _, _ = executed_statements[-1]
    assert sql == 'insert into "combo" ("name","val") values (?, ?)'


def test_upsert_rows_use_primary_key_disables_unique_fallback(
        upsert_conn, executed_statements):
    """Verify use_primary_key=True forbids the unique-index fallback.

    Mutation: `not use_primary_key` dropped from the fallback guard.
    Oracle: the fallback test's input with only the flag changed.
    """
    rows = ({'name': 'x', 'region': 'y', 'val': 3},)
    upsert_conn.upsert_rows(
        'combo', rows, update_cols_always=['val'],
        use_primary_key=True)

    sql, _, _ = executed_statements[-1]
    assert sql == 'insert into "combo" ("name","region","val") values (?, ?, ?)'


def test_upsert_rows_ignores_constraint_name_on_sqlite(
        upsert_conn, executed_statements):
    """Verify constraint_name is cleared for SQLite before column filtering.

    Mutation: `dialect != 'postgresql'` flipped to `==` in upsert_rows.
    Oracle: hand-written literal whose set list excludes the key column 'sku'.
    """
    rows = ({'sku': 'A', 'warehouse': 'W1', 'qty': 5},)
    upsert_conn.upsert_rows(
        'inventory', rows, constraint_name='uq_inventory',
        update_cols_always=['sku', 'qty'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'insert into "inventory" ("sku", "warehouse", "qty") '
        'values (?, ?, ?) '
        'on conflict ("sku", "warehouse") do update set "qty" = excluded."qty"'
        )
    assert sql == expected


def test_upsert_rows_do_nothing_leaves_existing_row_untouched(upsert_conn):
    """Verify an upsert with no update columns keeps the stored row.

    Mutation: the do-nothing early return deleted from the SQLite builder.
    Oracle: hand-computed state; qty stays 1 after a conflicting write of 9.
    """
    upsert_conn.upsert_rows(
        'inventory',
        ({'sku': 'A', 'warehouse': 'W', 'qty': 1},))
    upsert_conn.upsert_rows(
        'inventory',
        ({'sku': 'A', 'warehouse': 'W', 'qty': 9},))

    stored = db.select_scalar(upsert_conn, 'select qty from inventory')
    assert stored == 1


def test_upsert_rows_coalesce_fills_only_null_targets(upsert_conn):
    """Verify an ifnull column overwrites null but never an existing value.

    Mutation: the coalesce arguments swapped in _build_update_exprs.
    Oracle: two rows, one stored note null; only that one changes.
    """
    upsert_conn.upsert_rows('inventory', (
        {'sku': 'A', 'warehouse': 'W', 'qty': 1, 'note': 'orig'},
        {'sku': 'B', 'warehouse': 'W', 'qty': 1, 'note': None},
        ))
    upsert_conn.upsert_rows('inventory', (
        {'sku': 'A', 'warehouse': 'W', 'qty': 9, 'note': 'new'},
        {'sku': 'B', 'warehouse': 'W', 'qty': 9, 'note': 'new'},
        ), update_cols_always=['qty'], update_cols_ifnull=['note'])

    notes = db.select_column(
        upsert_conn,
        'select note from inventory order by sku')
    quantities = db.select_column(
        upsert_conn,
        'select qty from inventory order by sku')
    assert notes == ['orig', 'new']
    assert quantities == [9, 9]


def test_upsert_rows_without_primary_key_falls_back_to_insert(
        upsert_conn, executed_statements):
    """Verify a keyless table degrades to a plain insert.

    Mutation: the keyless insert_rows fallback replaced by `return 0`.
    Oracle: hand-written insert literal with insert_rows' comma spacing.
    """
    upsert_conn.upsert_rows('eventlog', ({'a': '1', 'b': '2'},),
                            update_cols_always=['b'])

    sql, params, _ = executed_statements[-1]
    assert sql == 'insert into "eventlog" ("a","b") values (?, ?)'
    assert params == [['1', '2']]


def test_upsert_rows_constraint_name_uses_resolved_expression(constraint_lookups):
    """Verify the resolved constraint expression becomes the conflict target.

    Mutation: `and not constraint_name` dropped from the insert fallback guard.
    Oracle: hand-written literal and a lookup spy; the rows omit key 'id'.
    """
    cn = StubConnection('postgresql', ['id', 'name', 'email'], ['id'])
    cn.upsert_rows('users', ({'name': 'a', 'email': 'e'},),
                   constraint_name='uq_users_name', update_cols_always=['email'])

    sql, params, _ = cn.recorder.calls[-1]
    expected = (
        'insert into "users" ("name", "email") values (%s, %s) '
        'on conflict (lower("name")) WHERE "email" IS NOT NULL '
        'do update set "email" = excluded."email"'
        )
    assert sql == expected
    assert params == [['a', 'e']]
    assert constraint_lookups == [('users', 'uq_users_name')]


def test_upsert_rows_constraint_name_permits_key_columns_in_update(
        constraint_lookups):
    """Verify a named constraint keeps primary-key columns in the set list.

    Mutation: key_cols_lower subtracted from updatable_lower in every case.
    Oracle: hand-written literal keeping the 'id' assignment.
    """
    cn = StubConnection('postgresql', ['id', 'name', 'email'], ['id'])
    cn.upsert_rows('users', ({'id': 1, 'name': 'a', 'email': 'e'},),
                   constraint_name='uq_users_name',
                   update_cols_always=['id', 'email'])

    sql, _, _ = cn.recorder.calls[-1]
    expected = (
        'insert into "users" ("id", "name", "email") values (%s, %s, %s) '
        'on conflict (lower("name")) WHERE "email" IS NOT NULL '
        'do update set "id" = excluded."id", "email" = excluded."email"'
        )
    assert sql == expected


def test_upsert_rows_returns_cursor_rowcount():
    """Verify the return value is the cursor rowcount.

    Mutation: total_affected replaced by len(rows) in upsert_rows.
    Oracle: a stub cursor reporting 7 for 3 rows.
    """
    cn = StubConnection('postgresql', ['id', 'v'], ['id'], rowcount=7)
    rows = tuple({'id': i, 'v': i} for i in range(3))

    assert cn.upsert_rows('t', rows, update_cols_always=['v']) == 7


def test_upsert_rows_forwards_batch_size():
    """Verify batch_size reaches the cursor.

    Mutation: batch_size dropped from the executemany call in upsert_rows.
    Oracle: a stub cursor recording the third positional argument.
    """
    cn = StubConnection('postgresql', ['id', 'v'], ['id'])
    rows = tuple({'id': i, 'v': i} for i in range(5))
    cn.upsert_rows('t', rows, update_cols_always=['v'], batch_size=2)

    assert cn.recorder.calls[-1][2] == 2


def test_upsert_rows_reset_sequence_runs_only_when_requested():
    """Verify the sequence reset runs on True only.

    Mutation: `if reset_sequence:` inverted in upsert_rows.
    Oracle: a spy recording reset_table_sequence calls for both flag values.
    """
    quiet = StubConnection('postgresql', ['id', 'v'], ['id'])
    quiet.upsert_rows('t', ({'id': 1, 'v': 2},), update_cols_always=['v'])
    assert quiet.sequence_resets == []

    loud = StubConnection('postgresql', ['id', 'v'], ['id'])
    loud.upsert_rows('t', ({'id': 1, 'v': 2},), update_cols_always=['v'],
                     reset_sequence=True)
    assert loud.sequence_resets == ['t']


def test_upsert_rows_rejects_constraint_name_with_conflict_columns():
    """Verify the two conflict-target arguments cannot both be supplied.

    Mutation: the mutual-exclusion check dropped from upsert_rows.
    Oracle: the exception type and message, with no statement issued.
    """
    cn = StubConnection('postgresql', ['id', 'name'], ['id'])
    with pytest.raises(ValidationError, match='mutually exclusive'):
        cn.upsert_rows('t', ({'id': 1, 'name': 'a'},),
                       constraint_name='uq_t', conflict_columns=['name'])

    assert cn.recorder.calls == []


def test_upsert_rows_unknown_columns_only_issues_no_statement():
    """Verify all-unknown keys or empty input issue no statement.

    Mutation: `if not columns:` changed to test table_columns.
    Oracle: a recording stub cursor stays empty for both inputs.
    """
    cn = StubConnection('postgresql', ['id', 'v'], ['id'])

    assert cn.upsert_rows('t', ({'nope': 1},), update_cols_always=['v']) == 0
    assert cn.recorder.calls == []

    assert cn.upsert_rows('t', (), update_cols_always=['v']) == 0
    assert cn.recorder.calls == []


def test_upsert_rows_missing_key_binds_null_parameter():
    """Verify a row omitting a column binds null and raises no KeyError.

    Mutation: row.get(col) changed to row[col] in upsert_rows.
    Oracle: hand-written parameter lists with None filling the gap.
    """
    cn = StubConnection('postgresql', ['id', 'name', 'email'], ['id'])
    rows = (
        {'id': 1, 'name': 'ann', 'email': 'ann@x'},
        {'id': 2, 'name': 'bob'},
        )
    affected = cn.upsert_rows(
        't', rows,
        update_cols_always=['name', 'email'])

    sql, params, _ = cn.recorder.calls[-1]
    expected = (
        'insert into "t" ("id", "name", "email") values (%s, %s, %s) '
        'on conflict ("id") do update set '
        '"name" = excluded."name", "email" = excluded."email"'
        )
    assert sql == expected
    assert params == [[1, 'ann', 'ann@x'], [2, 'bob', None]]
    assert affected == 2


def test_insert_rows_column_and_parameter_order():
    """Verify insert_rows emits one placeholder per column, values aligned.

    Mutation: make_placeholders given len(rows) in place of len(cols).
    Oracle: hand-written literal with three columns and two rows.
    """
    cn = StubConnection('postgresql', ['id', 'name', 'email'], ['id'])
    rows = (
        {'id': 1, 'name': 'a', 'email': 'x'},
        {'id': 2, 'name': 'b', 'email': 'y'},
        )
    cn.insert_rows('users', rows)

    sql, params, _ = cn.recorder.calls[-1]
    assert sql == 'insert into "users" ("id","name","email") values (%s, %s, %s)'
    assert params == [[1, 'a', 'x'], [2, 'b', 'y']]


def test_insert_rows_binds_each_row_by_column_name(upsert_conn):
    """Verify a row whose keys come in another order stores by name.

    Mutation: each row bound by row.values() in place of by column name.
    Oracle: hand-written stored pairs; the second row lists b before a.
    """
    upsert_conn.insert_rows('eventlog', [
        {'a': '1', 'b': '2'},
        {'b': '3', 'a': '4'},
        ])

    stored = db.select(
        upsert_conn, 'select a, b from eventlog order by a')
    assert stored.values.tolist() == [['1', '2'], ['4', '3']]


def test_insert_rows_missing_key_binds_null_parameter():
    """Verify a key absent from some rows still gets a column, bound null.

    Mutation: the column list taken from rows[0] alone, or row[col].
    Oracle: hand-written literal and parameter lists; 'email' first
        appears in the third row.
    """
    cn = StubConnection('postgresql', ['id', 'name', 'email'], ['id'])
    rows = (
        {'id': 1, 'name': 'a'},
        {'id': 2},
        {'id': 3, 'email': 'z'},
        )
    cn.insert_rows('users', rows)

    sql, params, _ = cn.recorder.calls[-1]
    assert sql == 'insert into "users" ("id","name","email") values (%s, %s, %s)'
    assert params == [[1, 'a', None], [2, None, None], [3, None, 'z']]


def test_insert_rows_with_only_unknown_keys_issues_no_statement():
    """Verify rows holding no table column insert nothing and return 0.

    Mutation: the guard testing the filtered list's length in place of
        whether any filtered row kept a key.
    Oracle: a recording stub cursor stays empty.
    """
    cn = StubConnection('postgresql', ['id', 'name'], ['id'])

    assert cn.insert_rows('t', ({'bogus': 1}, {'other': 2})) == 0
    assert cn.recorder.calls == []


@pytest.mark.parametrize(('dialect', 'marker'), [
    ('postgresql', '%s'),
    ('sqlite', '?'),
])
def test_insert_row_placeholder_style_per_dialect(dialect, marker):
    """Verify insert_row takes its placeholder marker from the connection.

    Mutation: 'postgresql' hardcoded in insert_row's make_placeholders call.
    Oracle: hand-written literal per dialect.
    """
    cn = StubConnection(dialect, ['id', 'name'], ['id'])
    cn.insert_row('users', ['id', 'name'], [1, 'a'])

    sql, args = cn.executed[-1]
    assert sql == f'insert into "users" ("id", "name") values ({marker}, {marker})'
    assert args == (1, 'a')


def test_update_row_sets_data_before_keys():
    """Verify the set list, the and-joined where, and the value order.

    Mutation: the value tuple order reversed, or the where joined by ' or '.
    Oracle: hand-written literal and argument tuple; two keys, two data fields.
    """
    cn = StubConnection('postgresql', ['a', 'b', 'c', 'd'], ['a', 'b'])
    cn.update_row('t', ['a', 'b'], [1, 2], ['c', 'd'], [3, 4])

    sql, args = cn.executed[-1]
    assert sql == 'update "t" set "c"=%s,"d"=%s where "a"=%s and "b"=%s'
    assert args == (3, 4, 1, 2)


def test_update_row_rejects_overlapping_and_mismatched_fields():
    """Verify update_row refuses a key field in datafields or ragged inputs.

    Mutation: the `kf in datafields` check dropped from update_row.
    Oracle: three rejected calls, none sending a statement.
    """
    cn = StubConnection('postgresql', ['a', 'b'], ['a'])

    with pytest.raises(ValidationError, match='cannot be in datafields'):
        cn.update_row('t', ['a'], [1], ['a', 'b'], [2, 3])
    with pytest.raises(ValidationError, match='keyfields'):
        cn.update_row('t', ['a', 'b'], [1], ['b'], [2])
    with pytest.raises(ValidationError, match='datafields'):
        cn.update_row('t', ['a'], [1], ['b'], [2, 3])

    assert cn.executed == []


if __name__ == '__main__':
    pytest.main([__file__])
