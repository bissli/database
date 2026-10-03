"""Named parameters on SQLite, in every named form, across statements.
"""
import database as db
import pytest


def test_named_params_bind_per_statement(sl_conn):
    """Verify each statement of a multi-statement execute binds its own names.

    Mutation: _execute_multi_statement_named matching only '%(name)s'.
    Oracle: hand-chosen values per statement, keys out of statement order.
    """
    sql = """
update test_table set value = %(alice)s where name = 'Alice';
update test_table set value = %(bob)s where name = 'Bob'
"""
    db.execute(sl_conn, sql, {'bob': 22, 'alice': 11})

    values = db.select_column(sl_conn, 'select value from test_table order by name')
    assert values == [11, 22, 30]


def test_named_in_expansion_binds_across_statements(sl_conn):
    """Verify an expanded named in list binds in a multi-statement execute.

    Mutation: a per-statement name filter dropping the ':names_0' keys.
    Oracle: hand-computed rows left after deleting Alice and Bob.
    """
    sql = """
delete from test_table where name in %(names)s;
update test_table set value = %(value)s where name = 'Charlie'
"""
    db.execute(sl_conn, sql, {'value': 33, 'names': ('Alice', 'Bob')})

    rows = db.select(sl_conn, 'select name, value from test_table')
    assert rows.to_dict('records') == [{'name': 'Charlie', 'value': 33}]


def test_raw_colon_params_bind_per_statement_on_cursor(sl_conn):
    """Verify Cursor.execute binds caller-written ':name' per statement.

    Mutation: the cursor's per-statement name pattern matching pyformat only.
    Oracle: hand-chosen values per statement, keys out of statement order.
    """
    sql = """
update test_table set value = :alice where name = 'Alice';
update test_table set value = :bob where name = 'Bob'
"""
    sl_conn.cursor().execute(sql, {'bob': 22, 'alice': 11})

    values = db.select_column(sl_conn, 'select value from test_table order by name')
    assert values == [11, 22, 30]


def test_dollar_and_at_params_bind_per_statement(sl_conn):
    """Verify '$name' and '@name' bind per statement across statements.

    Mutation: a per-statement name filter that recognizes only ':name'.
    Oracle: hand-chosen values per statement, keys out of statement order.
    """
    sql = """
update test_table set value = $alice where name = 'Alice';
update test_table set value = @bob where name = 'Bob'
"""
    db.execute(sl_conn, sql, {'bob': 22, 'alice': 11})

    values = db.select_column(sl_conn, 'select value from test_table order by name')
    assert values == [11, 22, 30]


def test_dollar_only_params_bind(sl_conn):
    """Verify a statement whose only placeholders are '$name' binds its dict.

    Mutation: the no-placeholder check running before the named check.
    Oracle: hand-chosen value read back for the named row.
    """
    sql = 'update test_table set value = $value where name = $who'
    db.execute(sl_conn, sql, {'value': 11, 'who': 'Alice'})

    values = db.select_column(sl_conn, 'select value from test_table order by name')
    assert values == [11, 20, 30]


def test_pyformat_mixed_with_native_named_keeps_every_key(sl_conn):
    """Verify prepare_query keeps a key that only a native '$name' uses.

    Mutation: prepare_query rebuilding the dict from pyformat names only.
    Oracle: hand-chosen value read back for the named row.
    """
    sql = 'update test_table set value = %(value)s where name = $who'
    db.execute(sl_conn, sql, {'value': 11, 'who': 'Alice'})

    values = db.select_column(sl_conn, 'select value from test_table order by name')
    assert values == [11, 20, 30]


def test_numbered_param_mixed_with_named_binds(sl_conn):
    """Verify a numbered '?1' beside a named placeholder binds by its number.

    Mutation: prepare_query writing ':None1' for the unnamed '?1'.
    Oracle: hand-chosen value read back for the named row.
    """
    sql = 'update test_table set value = ?1 where name = %(who)s'
    db.execute(sl_conn, sql, {'1': 11, 'who': 'Alice'})

    values = db.select_column(sl_conn, 'select value from test_table order by name')
    assert values == [11, 20, 30]


@pytest.mark.parametrize('column', ['`$x`', '[@x]'])
def test_quoted_identifier_sigil_keeps_dict_value_positional(sl_conn, column):
    """Verify a '$' or '@' inside a quoted identifier binds nothing by name.

    Mutation: _protected_ranges leaving backtick or bracket names unmasked.
    Oracle: the dict stored as the JSON text json.dumps writes.
    """
    db.execute(sl_conn, 'create table j (id integer, "$x" text, "@x" text, data text)')
    db.execute(
        sl_conn, f'insert into j (id, {column}, data) values (?, 1, ?)', 6, {'k': 'v'})

    assert db.select_scalar(sl_conn, 'select data from j where id = 6') == '{"k": "v"}'


def test_pyformat_params_bind_on_cursor_execute(sl_conn):
    """Verify Cursor.execute binds '%(name)s' with a dict on SQLite.

    Mutation: standardize_placeholders keeping '%(name)s' for sqlite.
    Oracle: hand-chosen value read back for the named row.
    """
    sql = 'update test_table set value = %(value)s where name = %(who)s'
    sl_conn.cursor().execute(sql, {'value': 11, 'who': 'Alice'})

    values = db.select_column(sl_conn, 'select value from test_table order by name')
    assert values == [11, 20, 30]


def test_pyformat_params_bind_on_cursor_executemany(sl_conn):
    """Verify Cursor.executemany binds '%(name)s' from a list of dicts.

    Mutation: executemany skipping standardize_sql.
    Oracle: hand-chosen values per row, keys out of placeholder order.
    """
    sql = 'update test_table set value = %(value)s where name = %(who)s'
    params = [{'who': 'Alice', 'value': 11}, {'who': 'Bob', 'value': 22}]
    sl_conn.cursor().executemany(sql, params)

    values = db.select_column(sl_conn, 'select value from test_table order by name')
    assert values == [11, 22, 30]
