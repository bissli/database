"""Unit tests for generated SQL: SELECT/INSERT builders, strategy upsert
statements, and the upsert_rows orchestration in ConnectionWrapper.

Every expected statement here is an independently written literal, never a
re-derivation using the joins the source itself uses.
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

    The SQL-building methods are the production ones. Only the two schema
    lookups, the cursor, and execute() are replaced, so a mutation anywhere
    in the statement-building path still reaches the assertions.
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

    inventory - composite primary key, no unique index.
    combo     - surrogate primary key plus a two-column unique index.
    eventlog  - no primary key at all.
    """
    cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    db.execute(cn, """
    CREATE TABLE inventory (
        sku TEXT NOT NULL,
        warehouse TEXT NOT NULL,
        qty INTEGER,
        note TEXT,
        PRIMARY KEY (sku, warehouse)
    )
    """)
    db.execute(cn, """
    CREATE TABLE combo (
        id INTEGER PRIMARY KEY,
        name TEXT,
        region TEXT,
        val INTEGER,
        UNIQUE (name, region)
    )
    """)
    db.execute(cn, 'CREATE TABLE eventlog (a TEXT, b TEXT)')
    yield cn
    cn.close()


@pytest.fixture
def executed_statements(monkeypatch):
    """Record every (sql, params, batch_size) reaching Cursor.executemany.

    The original still runs, so a recorded statement is also proved valid
    against real SQLite.
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
    """Verify build_select_sql() emits SELECT * only when columns is empty.

    Mutation: swapping the `if columns:` branches in build_select_sql so an
    empty list yields an empty column list instead of '*'.
    Oracle: hand-written literals for both the empty and non-empty case.
    """
    assert build_select_sql('users', dialect) == 'SELECT * FROM "users"'
    assert build_select_sql('users', dialect, columns=[]) == 'SELECT * FROM "users"'
    assert (build_select_sql('users', dialect, columns=['id'])
            == 'SELECT "id" FROM "users"')


@pytest.mark.parametrize('dialect', ['postgresql', 'sqlite'])
def test_select_quotes_each_identifier(dialect):
    """Verify every column and the table are quoted separately, in order.

    Mutation: quoting the column list as one identifier in build_select_sql,
    or dropping the schema split in quote_identifier.
    Oracle: hand-written literal with a dotted table and a spaced column.
    """
    sql = build_select_sql('public.users', dialect, columns=['id', 'full name'])
    assert sql == 'SELECT "id", "full name" FROM "public"."users"'


def test_select_where_precedes_order_by():
    """Verify WHERE is appended before ORDER BY and neither is quoted.

    Mutation: reordering the clause appends in build_select_sql so ORDER BY
    lands ahead of WHERE.
    Oracle: hand-written literal for the full statement.
    """
    sql = build_select_sql(
        'users', 'postgresql', where='active = TRUE',
        order_by='"name" DESC')
    assert sql == 'SELECT * FROM "users" WHERE active = TRUE ORDER BY "name" DESC'


def test_select_limit_zero_is_emitted():
    """Verify LIMIT 0 survives while limit=None emits no LIMIT.

    Mutation: `if limit is not None:` weakened to `if limit:` in
    build_select_sql, which silently drops LIMIT 0.
    Oracle: boundary pair straddling the falsy/None distinction.
    """
    assert (build_select_sql('users', 'postgresql', limit=0)
            == 'SELECT * FROM "users" LIMIT 0')
    assert (build_select_sql('users', 'postgresql', limit=10)
            == 'SELECT * FROM "users" LIMIT 10')
    assert build_select_sql('users', 'postgresql') == 'SELECT * FROM "users"'


@pytest.mark.parametrize(('dialect', 'expected'), [
    ('postgresql', 'INSERT INTO "users" ("id", "name") VALUES (%s, %s)'),
    ('sqlite', 'INSERT INTO "users" ("id", "name") VALUES (?, ?)'),
    ], ids=['postgresql', 'sqlite'])
def test_insert_placeholder_marker_per_dialect(dialect, expected):
    """Verify the placeholder marker follows the dialect, one per column.

    Mutation: flipping the ternary in make_placeholders so '?' and '%s' swap
    dialects.
    Oracle: hand-written literal per dialect.
    """
    assert build_insert_sql(
        dialect=dialect, table='users',
        columns=['id', 'name']) == expected


class TestSQLGeneration:
    """Full-statement pins for the sql.py builders.
    """

    @pytest.mark.parametrize('dialect', ['postgresql', 'sqlite'])
    def test_build_select_sql(self, dialect):
        """Verify all four optional clauses render in SQL order.

        Mutation: moving the LIMIT append above the ORDER BY append in
        build_select_sql.
        Oracle: hand-written literal for the whole statement.
        """
        sql = build_select_sql(
            'users', dialect, columns=['id', 'name'],
            where='active = true', order_by='"name"', limit=10)
        expected = (
            'SELECT "id", "name" FROM "users" '
            'WHERE active = true ORDER BY "name" LIMIT 10'
            )
        assert sql == expected

    def test_build_insert_sql(self):
        """Verify a dotted table splits and an embedded quote is doubled.

        Mutation: replacing quote_identifier's per-segment quoting with a
        single wrap, giving '"main.users"' and '"na"me"'.
        Oracle: hand-written literal following the SQL escaping rule.
        """
        sql = build_insert_sql(
            dialect='sqlite', table='main.users',
            columns=['id', 'na"me'])
        assert sql == 'INSERT INTO "main"."users" ("id", "na""me") VALUES (?, ?)'


@pytest.mark.parametrize(('dialect', 'marker'), [
    ('postgresql', '%s'),
    ('sqlite', '?'),
])
def test_upsert_sql_do_nothing_without_update_columns(dialect, marker):
    """Verify no update columns yields DO NOTHING on the key column list.

    Mutation: deleting the `if not (update_cols_always or update_cols_ifnull):
    return ... DO NOTHING` early return in postgres.py:409-410 /
    sqlite.py:291-292, so the function emits a trailing `DO UPDATE SET ` with
    an empty expression list.
    Oracle: hand-written literal per dialect.
    """
    sql = get_strategy(dialect).build_upsert_sql(
        table='mytable',
        columns=['a', 'b', 'c'],
        key_columns=['a', 'b'])
    expected = (
        f'INSERT INTO "mytable" ("a", "b", "c") '
        f'VALUES ({marker}, {marker}, {marker}) '
        f'ON CONFLICT ("a", "b") DO NOTHING'
        )
    assert sql == expected


@pytest.mark.parametrize(('dialect', 'marker'), [
    ('postgresql', '%s'),
    ('sqlite', '?'),
])
def test_upsert_sql_do_update_set_exact(dialect, marker):
    """Verify always-updated columns render as col = excluded.col, in order.

    Mutation: building the conflict target from `quoted_columns` instead of
    `quoted_keys` in build_upsert_sql, so every column becomes a key.
    Oracle: hand-written literal per dialect.
    """
    sql = get_strategy(dialect).build_upsert_sql(
        table='mytable',
        columns=['a', 'b', 'c'],
        key_columns=['a'],
        update_cols_always=['c', 'b'])
    expected = (
        f'INSERT INTO "mytable" ("a", "b", "c") '
        f'VALUES ({marker}, {marker}, {marker}) '
        f'ON CONFLICT ("a") '
        f'DO UPDATE SET "c" = excluded."c", "b" = excluded."b"'
        )
    assert sql == expected


def test_upsert_sql_coalesce_prefers_existing_row():
    """Verify the ifnull column coalesces the stored value ahead of excluded.

    Mutation: swapping the COALESCE arguments in _build_update_exprs, which
    would overwrite every non-null stored value.
    Oracle: hand-written literal; argument order decides the semantics.
    """
    sql = get_strategy('postgresql').build_upsert_sql(
        table='mytable',
        columns=['a', 'b'],
        key_columns=['a'],
        update_cols_ifnull=['b'])
    expected = (
        'INSERT INTO "mytable" ("a", "b") VALUES (%s, %s) '
        'ON CONFLICT ("a") '
        'DO UPDATE SET "b" = COALESCE("mytable"."b", excluded."b")'
        )
    assert sql == expected


def test_upsert_sql_always_columns_precede_ifnull_columns():
    """Verify the SET list runs always-columns first, then ifnull-columns.

    Mutation: appending the ifnull block before the always block in
    _build_update_exprs.
    Oracle: hand-written literal with one column of each kind.
    """
    sql = get_strategy('postgresql').build_upsert_sql(
        table='t',
        columns=['k', 'x', 'y'],
        key_columns=['k'],
        update_cols_always=['x'],
        update_cols_ifnull=['y'])
    expected = (
        'INSERT INTO "t" ("k", "x", "y") VALUES (%s, %s, %s) '
        'ON CONFLICT ("k") '
        'DO UPDATE SET "x" = excluded."x", '
        '"y" = COALESCE("t"."y", excluded."y")'
        )
    assert sql == expected


def test_upsert_sql_constraint_expression_replaces_column_target():
    """Verify a constraint expression is used verbatim and key columns are not.

    Mutation: dropping the `if constraint_expr:` branch in the PostgreSQL
    build_upsert_sql so the key column list is emitted instead.
    Oracle: hand-written literal carrying a partial-index WHERE clause that
    a column list could never produce.
    """
    sql = get_strategy('postgresql').build_upsert_sql(
        table='users',
        columns=['id', 'name'],
        key_columns=['id'],
        constraint_expr='(lower("name")) WHERE "name" IS NOT NULL',
        update_cols_always=['name'])
    expected = (
        'INSERT INTO "users" ("id", "name") VALUES (%s, %s) '
        'ON CONFLICT (lower("name")) WHERE "name" IS NOT NULL '
        'DO UPDATE SET "name" = excluded."name"'
        )
    assert sql == expected


def test_upsert_sql_sqlite_ignores_constraint_expression():
    """Verify SQLite builds the conflict target from key columns regardless.

    Mutation: honoring constraint_expr in the SQLite build_upsert_sql, which
    would emit a PostgreSQL-only named-constraint target.
    Oracle: hand-written literal; the same call on the PostgreSQL strategy
    differs, so the two are compared against each other.
    """
    kwargs = {
        'table': 't',
        'columns': ['k', 'v'],
        'key_columns': ['k'],
        'constraint_expr': 'ON CONSTRAINT uq_t',
        'update_cols_always': ['v'],
        }
    sqlite_sql = get_strategy('sqlite').build_upsert_sql(**kwargs)
    expected = (
        'INSERT INTO "t" ("k", "v") VALUES (?, ?) '
        'ON CONFLICT ("k") DO UPDATE SET "v" = excluded."v"'
        )
    assert sqlite_sql == expected
    assert 'uq_t' in get_strategy('postgresql').build_upsert_sql(**kwargs)


def test_upsert_sql_quotes_schema_qualified_table():
    """Verify a dotted table is split in both the INSERT and the COALESCE.

    Mutation: quoting the table as one identifier in _build_update_exprs,
    giving COALESCE("public.t"."b", ...).
    Oracle: hand-written literal with the schema split applied twice.
    """
    sql = get_strategy('postgresql').build_upsert_sql(
        table='public.t',
        columns=['a', 'b'],
        key_columns=['a'],
        update_cols_ifnull=['b'])
    expected = (
        'INSERT INTO "public"."t" ("a", "b") VALUES (%s, %s) '
        'ON CONFLICT ("a") '
        'DO UPDATE SET "b" = COALESCE("public"."t"."b", excluded."b")'
        )
    assert sql == expected


def test_upsert_rows_uses_table_column_order(upsert_conn, executed_statements):
    """Verify insert columns and parameters follow table order, not dict order.

    Mutation: `columns = tuple(col for col in table_columns if ...)` replaced
    by a sort or by the caller's key order in upsert_rows.
    Oracle: row dict written in an order that matches neither table order nor
    alphabetical order, checked against a hand-written literal.
    """
    rows = ({'note': 'n', 'qty': 5, 'warehouse': 'W1', 'sku': 'A'},)
    upsert_conn.upsert_rows('inventory', rows, update_cols_always=['qty'])

    sql, params, _ = executed_statements[-1]
    expected = (
        'INSERT INTO "inventory" ("sku", "warehouse", "qty", "note") '
        'VALUES (?, ?, ?, ?) '
        'ON CONFLICT ("sku", "warehouse") DO UPDATE SET "qty" = excluded."qty"'
        )
    assert sql == expected
    assert params == [['A', 'W1', 5, 'n']]


def test_upsert_rows_corrects_case_and_drops_unknown_columns(
        upsert_conn, executed_statements):
    """Verify column names are case-corrected and unknown keys are dropped.

    Mutation: dropping the case_map lookup in filter_table_columns so 'QTY'
    is kept verbatim.
    Oracle: hand-written literal naming the lower-case schema columns only.
    """
    rows = ({'SKU': 'A', 'Warehouse': 'W1', 'QTY': 5, 'bogus': 1},)
    upsert_conn.upsert_rows('inventory', rows, update_cols_always=['QTY'])

    sql, params, _ = executed_statements[-1]
    expected = (
        'INSERT INTO "inventory" ("sku", "warehouse", "qty") '
        'VALUES (?, ?, ?) '
        'ON CONFLICT ("sku", "warehouse") DO UPDATE SET "qty" = excluded."qty"'
        )
    assert sql == expected
    assert params == [['A', 'W1', 5]]


def test_upsert_rows_omits_key_columns_from_update_set(
        upsert_conn, executed_statements):
    """Verify a key column named in update_cols_always is filtered out.

    Mutation: dropping the `lower not in key_cols_lower` guard on the
    update_cols_always filter in upsert_rows.
    Oracle: hand-written literal whose SET list holds only the non-key column.
    """
    rows = ({'sku': 'A', 'warehouse': 'W1', 'qty': 5},)
    upsert_conn.upsert_rows(
        'inventory', rows,
        update_cols_always=['sku', 'qty', 'warehouse'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'INSERT INTO "inventory" ("sku", "warehouse", "qty") '
        'VALUES (?, ?, ?) '
        'ON CONFLICT ("sku", "warehouse") DO UPDATE SET "qty" = excluded."qty"'
        )
    assert sql == expected


def test_upsert_rows_ifnull_skips_columns_already_always_updated(
        upsert_conn, executed_statements):
    """Verify a column in both update lists is set once, unconditionally.

    Mutation: dropping the `lower not in uc_always_lower` guard on the
    update_cols_ifnull filter, which would emit the column twice.
    Oracle: hand-written literal with one assignment per column.
    """
    rows = ({'sku': 'A', 'warehouse': 'W1', 'qty': 5, 'note': 'n'},)
    upsert_conn.upsert_rows(
        'inventory', rows,
        update_cols_always=['qty'],
        update_cols_ifnull=['qty', 'note'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'INSERT INTO "inventory" ("sku", "warehouse", "qty", "note") '
        'VALUES (?, ?, ?, ?) '
        'ON CONFLICT ("sku", "warehouse") '
        'DO UPDATE SET "qty" = excluded."qty", '
        '"note" = COALESCE("inventory"."note", excluded."note")'
        )
    assert sql == expected


def test_upsert_rows_ifnull_excludes_key_columns(
        upsert_conn, executed_statements):
    """Verify key columns listed in update_cols_ifnull are filtered out.

    Mutation: dropping `lower not in key_cols_lower` from the
    update_cols_ifnull filter in upsert_rows (connection.py:738), which would
    emit `"sku" = COALESCE("inventory"."sku", excluded."sku")` in the SET list.
    Oracle: hand-written literal whose SET list holds only the non-key column.
    """
    rows = ({'sku': 'A', 'warehouse': 'W1', 'qty': 1, 'note': 'n'},)
    upsert_conn.upsert_rows('inventory', rows, update_cols_ifnull=['sku', 'note'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'INSERT INTO "inventory" ("sku", "warehouse", "qty", "note") '
        'VALUES (?, ?, ?, ?) '
        'ON CONFLICT ("sku", "warehouse") '
        'DO UPDATE SET "note" = COALESCE("inventory"."note", excluded."note")'
        )
    assert sql == expected


def test_filter_table_columns_drops_unknown_keys():
    """Verify filter_table_columns silently drops keys absent from the schema.

    Mutation: making filter_table_columns keep unknown keys verbatim
    (`else: filtered_row[col] = val`), which would include 'bogus' in the
    INSERT column list.
    Oracle: hand-written INSERT literal with two schema columns only; 'bogus'
    would appear as a third column if not dropped.
    """
    cn = StubConnection('postgresql', ['id', 'name'], ['id'])
    cn.insert_rows('t', ({'id': 1, 'name': 'a', 'bogus': 9},))

    sql, params, _ = cn.recorder.calls[-1]
    assert sql == 'INSERT INTO "t" ("id","name") VALUES (%s, %s)'
    assert params == [[1, 'a']]


def test_upsert_rows_conflict_columns_override_primary_key(
        upsert_conn, executed_statements):
    """Verify conflict_columns replace the primary key and are case-corrected.

    Mutation: `key_cols = [case_map.get(c.lower(), c) for c in
    conflict_columns]` replaced by the raw caller list, giving
    ON CONFLICT ("NAME").
    Oracle: hand-written literal; SQLite would reject the uncorrected name.
    """
    rows = ({'id': 1, 'name': 'x', 'region': 'y', 'val': 3},)
    upsert_conn.upsert_rows(
        'combo', rows, conflict_columns=['NAME', 'Region'],
        update_cols_always=['val', 'Name'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'INSERT INTO "combo" ("id", "name", "region", "val") '
        'VALUES (?, ?, ?, ?) '
        'ON CONFLICT ("name", "region") DO UPDATE SET "val" = excluded."val"'
        )
    assert sql == expected


def test_upsert_rows_uses_unique_index_when_primary_key_absent(
        upsert_conn, executed_statements):
    """Verify SQLite falls back to a unique index when the key is not supplied.

    Mutation: `not use_primary_key` flipped to `use_primary_key` in the
    unique-column fallback guard in upsert_rows.
    Oracle: hand-written literal targeting the two-column unique index; the
    primary key 'id' never appears.
    """
    rows = ({'name': 'x', 'region': 'y', 'val': 3},)
    upsert_conn.upsert_rows('combo', rows, update_cols_always=['val'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'INSERT INTO "combo" ("name", "region", "val") VALUES (?, ?, ?) '
        'ON CONFLICT ("name", "region") DO UPDATE SET "val" = excluded."val"'
        )
    assert sql == expected


def test_upsert_rows_requires_every_unique_column_present(
        upsert_conn, executed_statements):
    """Verify a partly supplied unique index is not used as a conflict target.

    Mutation: `all(...)` weakened to `any(...)` on the unique-column check in
    upsert_rows, which would target ("name", "region") without a region value.
    Oracle: hand-written plain INSERT literal; the boundary is one supplied
    column out of the index's two.
    """
    rows = ({'name': 'x', 'val': 3},)
    upsert_conn.upsert_rows('combo', rows, update_cols_always=['val'])

    sql, _, _ = executed_statements[-1]
    assert sql == 'INSERT INTO "combo" ("name","val") VALUES (?, ?)'


def test_upsert_rows_use_primary_key_disables_unique_fallback(
        upsert_conn, executed_statements):
    """Verify use_primary_key=True forbids the unique-index fallback.

    Mutation: dropping `not use_primary_key` from the fallback guard in
    upsert_rows, which would produce ON CONFLICT ("name", "region").
    Oracle: same input as the fallback test, so only the flag differs; the
    hand-written expectation is a plain INSERT.
    """
    rows = ({'name': 'x', 'region': 'y', 'val': 3},)
    upsert_conn.upsert_rows(
        'combo', rows, update_cols_always=['val'],
        use_primary_key=True)

    sql, _, _ = executed_statements[-1]
    assert sql == 'INSERT INTO "combo" ("name","region","val") VALUES (?, ?, ?)'


def test_upsert_rows_ignores_constraint_name_on_sqlite(
        upsert_conn, executed_statements):
    """Verify constraint_name is cleared for SQLite before column filtering.

    Mutation: `if dialect != 'postgresql': constraint_name = None` flipped to
    `==`, which leaves constraint_name set and lets key columns survive into
    the SET list.
    Oracle: hand-written literal whose SET list excludes the key column 'sku'.
    """
    rows = ({'sku': 'A', 'warehouse': 'W1', 'qty': 5},)
    upsert_conn.upsert_rows(
        'inventory', rows, constraint_name='uq_inventory',
        update_cols_always=['sku', 'qty'])

    sql, _, _ = executed_statements[-1]
    expected = (
        'INSERT INTO "inventory" ("sku", "warehouse", "qty") '
        'VALUES (?, ?, ?) '
        'ON CONFLICT ("sku", "warehouse") DO UPDATE SET "qty" = excluded."qty"'
        )
    assert sql == expected


def test_upsert_rows_do_nothing_leaves_existing_row_untouched(upsert_conn):
    """Verify an upsert with no update columns keeps the stored row.

    Mutation: deleting the `if not (update_cols_always or update_cols_ifnull):
    return ... DO NOTHING` early return in sqlite.py:291-292, so the
    conflicting write reaches DO UPDATE and overwrites the stored qty.
    Oracle: hand-computed table state - qty stays 1 after a conflicting
    write of 9.
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
    """Verify an ifnull column overwrites NULL but never an existing value.

    Mutation: swapping the COALESCE arguments in _build_update_exprs, which
    would replace 'orig' with 'new'.
    Oracle: hand-computed row pair - one stored note is NULL, the other is
    not, and only the NULL one changes.
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
    """Verify a keyless table degrades to a plain INSERT, not a broken upsert.

    Mutation: replacing `return self.insert_rows(table, rows)` at the keyless
    fallback in upsert_rows (connection.py:742) with `return 0`, silently
    dropping the rows instead of inserting them.
    Oracle: hand-written INSERT literal, which uses the comma spacing of
    insert_rows rather than build_upsert_sql.
    """
    upsert_conn.upsert_rows('eventlog', ({'a': '1', 'b': '2'},),
                            update_cols_always=['b'])

    sql, params, _ = executed_statements[-1]
    assert sql == 'INSERT INTO "eventlog" ("a","b") VALUES (?, ?)'
    assert params == [['1', '2']]


def test_upsert_rows_constraint_name_uses_resolved_expression(constraint_lookups):
    """Verify the resolved constraint expression becomes the conflict target.

    Mutation: `and (dialect != 'postgresql' or not constraint_name)` reduced
    so a named constraint no longer excuses an absent key column, which would
    fall back to a plain INSERT.
    Oracle: hand-written literal plus a spy recording the constraint lookup;
    the primary key 'id' is deliberately absent from the rows.
    """
    cn = StubConnection('postgresql', ['id', 'name', 'email'], ['id'])
    cn.upsert_rows('users', ({'name': 'a', 'email': 'e'},),
                   constraint_name='uq_users_name', update_cols_always=['email'])

    sql, params, _ = cn.recorder.calls[-1]
    expected = (
        'INSERT INTO "users" ("name", "email") VALUES (%s, %s) '
        'ON CONFLICT (lower("name")) WHERE "email" IS NOT NULL '
        'DO UPDATE SET "email" = excluded."email"'
        )
    assert sql == expected
    assert params == [['a', 'e']]
    assert constraint_lookups == [('users', 'uq_users_name')]


def test_upsert_rows_constraint_name_permits_key_columns_in_update(
        constraint_lookups):
    """Verify a named constraint keeps primary-key columns in the SET list.

    Mutation: dropping `constraint_name is not None or` from the
    update_cols_always filter in upsert_rows, which would silently discard
    the 'id' assignment.
    Oracle: hand-written literal; the same call without constraint_name drops
    'id', so the two branches disagree.
    """
    cn = StubConnection('postgresql', ['id', 'name', 'email'], ['id'])
    cn.upsert_rows('users', ({'id': 1, 'name': 'a', 'email': 'e'},),
                   constraint_name='uq_users_name',
                   update_cols_always=['id', 'email'])

    sql, _, _ = cn.recorder.calls[-1]
    expected = (
        'INSERT INTO "users" ("id", "name", "email") VALUES (%s, %s, %s) '
        'ON CONFLICT (lower("name")) WHERE "email" IS NOT NULL '
        'DO UPDATE SET "id" = excluded."id", "email" = excluded."email"'
        )
    assert sql == expected


def test_upsert_rows_returns_cursor_rowcount():
    """Verify the return value is the cursor rowcount, not the input length.

    Mutation: `total_affected = rc if isinstance(rc, int) else 0` replaced by
    `len(rows)` in upsert_rows.
    Oracle: a stub cursor reporting 7 for 3 supplied rows, so the two
    candidate answers cannot coincide.
    """
    cn = StubConnection('postgresql', ['id', 'v'], ['id'], rowcount=7)
    rows = tuple({'id': i, 'v': i} for i in range(3))

    assert cn.upsert_rows('t', rows, update_cols_always=['v']) == 7


def test_upsert_rows_forwards_batch_size():
    """Verify batch_size reaches the cursor rather than the 500 default.

    Mutation: dropping the batch_size argument from the
    `cursor.executemany(sql, params, batch_size)` call in upsert_rows.
    Oracle: a stub cursor recording the third positional argument.
    """
    cn = StubConnection('postgresql', ['id', 'v'], ['id'])
    rows = tuple({'id': i, 'v': i} for i in range(5))
    cn.upsert_rows('t', rows, update_cols_always=['v'], batch_size=2)

    assert cn.recorder.calls[-1][2] == 2


def test_upsert_rows_reset_sequence_runs_only_when_requested():
    """Verify the sequence reset fires on True and stays silent on False.

    Mutation: `if reset_sequence:` inverted in upsert_rows.
    Oracle: a spy recording reset_table_sequence calls across both flag
    values.
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

    Mutation: dropping the mutual-exclusion check in upsert_rows, which would
    let conflict_columns silently lose to constraint_name.
    Oracle: the raised exception type and message, with no statement issued.
    """
    cn = StubConnection('postgresql', ['id', 'name'], ['id'])
    with pytest.raises(ValidationError, match='mutually exclusive'):
        cn.upsert_rows('t', ({'id': 1, 'name': 'a'},),
                       constraint_name='uq_t', conflict_columns=['name'])

    assert cn.recorder.calls == []


def test_upsert_rows_unknown_columns_only_issues_no_statement():
    """Verify rows with all-unknown keys, or empty input, produce no statement.

    Mutation: `if not columns:` in upsert_rows changed to test table_columns
    instead, which would emit INSERT INTO "t" () VALUES ().
    Oracle: a stub cursor recording every call; the list stays empty for both
    an all-unknown-key row and an empty row tuple.
    """
    cn = StubConnection('postgresql', ['id', 'v'], ['id'])

    assert cn.upsert_rows('t', ({'nope': 1},), update_cols_always=['v']) == 0
    assert cn.recorder.calls == []

    assert cn.upsert_rows('t', (), update_cols_always=['v']) == 0
    assert cn.recorder.calls == []


def test_upsert_rows_missing_key_binds_null_parameter():
    """Verify a row omitting a column binds NULL instead of raising KeyError.

    Mutation: `row.get(col)` restored to `row[col]` in the upsert_rows
    parameter build, which raises KeyError on the row that omits 'email'.
    Oracle: hand-written parameter lists, both three long, with None filling
    the gap in the second row.
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
        'INSERT INTO "t" ("id", "name", "email") VALUES (%s, %s, %s) '
        'ON CONFLICT ("id") DO UPDATE SET '
        '"name" = excluded."name", "email" = excluded."email"'
        )
    assert sql == expected
    assert params == [[1, 'ann', 'ann@x'], [2, 'bob', None]]
    assert affected == 2


def test_upsert_rows_missing_key_stores_null(upsert_conn):
    """Verify the column one row omits reaches the table as NULL.

    Mutation: `row.get(col)` restored to `row[col]` in the upsert_rows
    parameter build, so the row omitting 'note' raises KeyError and neither
    row is written.
    Oracle: hand-computed table state read back in sku order - notes
    ['n', None] against quantities [1, 2].
    """
    upsert_conn.upsert_rows('inventory', (
        {'sku': 'A', 'warehouse': 'W', 'qty': 1, 'note': 'n'},
        {'sku': 'B', 'warehouse': 'W', 'qty': 2},
        ), update_cols_always=['qty'])

    notes = db.select_column(
        upsert_conn,
        'select note from inventory order by sku')
    quantities = db.select_column(
        upsert_conn,
        'select qty from inventory order by sku')
    assert notes == ['n', None]
    assert quantities == [1, 2]


def test_insert_rows_column_and_parameter_order():
    """Verify insert_rows emits one placeholder per column, values aligned.

    Mutation: `make_placeholders(len(cols), self.dialect)` taking len(rows)
    instead of len(cols) in insert_rows.
    Oracle: hand-written literal with three columns and two rows, so the two
    lengths differ.
    """
    cn = StubConnection('postgresql', ['id', 'name', 'email'], ['id'])
    rows = (
        {'id': 1, 'name': 'a', 'email': 'x'},
        {'id': 2, 'name': 'b', 'email': 'y'},
        )
    cn.insert_rows('users', rows)

    sql, params, _ = cn.recorder.calls[-1]
    assert sql == 'INSERT INTO "users" ("id","name","email") VALUES (%s, %s, %s)'
    assert params == [[1, 'a', 'x'], [2, 'b', 'y']]


@pytest.mark.parametrize(('dialect', 'marker'), [
    ('postgresql', '%s'),
    ('sqlite', '?'),
])
def test_insert_row_placeholder_style_per_dialect(dialect, marker):
    """Verify insert_row takes its placeholder marker from the connection.

    Mutation: hardcoding 'postgresql' in the make_placeholders call inside
    insert_row.
    Oracle: hand-written literal per dialect.
    """
    cn = StubConnection(dialect, ['id', 'name'], ['id'])
    cn.insert_row('users', ['id', 'name'], [1, 'a'])

    sql, args = cn.executed[-1]
    assert sql == f'INSERT INTO "users" ("id", "name") VALUES ({marker}, {marker})'
    assert args == (1, 'a')


def test_update_row_sets_data_before_keys():
    """Verify the SET list, the AND-joined WHERE, and the value order.

    Mutation: `values = tuple(datavalues) + tuple(keyvalues)` reversed in
    update_row, or the WHERE clause joined with ' or '.
    Oracle: hand-written literal plus a hand-ordered argument tuple; two key
    fields and two data fields keep the halves distinguishable.
    """
    cn = StubConnection('postgresql', ['a', 'b', 'c', 'd'], ['a', 'b'])
    cn.update_row('t', ['a', 'b'], [1, 2], ['c', 'd'], [3, 4])

    sql, args = cn.executed[-1]
    assert sql == 'update "t" set "c"=%s,"d"=%s where "a"=%s and "b"=%s'
    assert args == (3, 4, 1, 2)


def test_update_row_rejects_overlapping_and_mismatched_fields():
    """Verify update_row refuses a key field in datafields or ragged inputs.

    Mutation: dropping the `kf in datafields` check in update_row, which would
    emit an UPDATE that rewrites its own WHERE column.
    Oracle: three rejected calls; each raises before any statement is sent.
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
