import datetime
import sqlite3
from types import SimpleNamespace

import pytest
from database.types import Column, columns_from_cursor_description
from database.utils import get_dialect_name


@pytest.fixture
def pg_desc():
    """Factory for psycopg-style description items.

    The object exposes attributes only and refuses indexing, so a
    positional read of it raises instead of silently working.
    """
    def factory(
        name, type_code, display_size=None, internal_size=None,
        precision=None, scale=None):
        return SimpleNamespace(
            name=name,
            type_code=type_code,
            display_size=display_size,
            internal_size=internal_size,
            precision=precision,
            scale=scale)

    return factory


@pytest.fixture
def sqlite_memory():
    """In-memory sqlite3 connection, for real cursor descriptions."""
    cn = sqlite3.connect(':memory:')
    yield cn
    cn.close()


def test_postgres_description_attributes_land_in_matching_slots(pg_desc):
    """Verify each psycopg description attribute maps to its own Column slot.

    Mutation: swapping display_size and internal_size (or precision and
        scale) in Column._extract_postgres_column_info.
    Oracle: hand-written dict with a distinct value per slot.
    """
    desc = pg_desc(
        'amount', 1700, display_size=11, internal_size=8, precision=10,
        scale=2)
    column = Column.from_cursor_description(desc, 'postgresql')

    assert column.to_dict() == {
        'name': 'amount',
        'type_code': 1700,
        'python_type': 'float',
        'display_size': 11,
        'internal_size': 8,
        'precision': 10,
        'scale': 2,
        'nullable': None,
        }


def test_postgres_type_code_beats_column_name_pattern(pg_desc):
    """Verify the OID decides the Python type, not the column name suffix.

    Mutation: moving the column-name pattern block above the type-code
        lookup in resolve_type, or remapping 'date' to datetime.datetime
        in _build_postgres_types.
    Oracle: pg_type catalog OIDs 1082 (date) and 1114 (timestamp), whose
        names would otherwise resolve to datetime and time.
    """
    dated = Column.from_cursor_description(
        pg_desc('created_at', 1082), 'postgresql')
    stamped = Column.from_cursor_description(
        pg_desc('start_time', 1114), 'postgresql')

    assert dated.python_type is datetime.date
    assert stamped.python_type is datetime.datetime


def test_postgres_oid_map_covers_core_types(pg_desc):
    """Verify each core PostgreSQL OID resolves to its documented Python type.

    Mutation: retyping 'time' as datetime.datetime in _build_postgres_types,
        or mapping array OIDs to str instead of tuple.
    Oracle: pg_type catalog OIDs, each paired with a type that differs from
        the str fallback resolve_type would otherwise return.
    """
    oid_cases = [
        ('flag', 16, bool),
        ('payload', 17, bytes),
        ('total', 23, int),
        ('user_id', 25, str),
        ('wall', 1083, datetime.time),
        ('logged', 1184, datetime.datetime),
        ('ratio', 1700, float),
        ('doc', 3802, dict),
        ('tags', 1007, tuple),
        ]

    resolved = [
        Column.from_cursor_description(
            pg_desc(name, oid), 'postgresql').python_type
        for name, oid, _ in oid_cases
        ]

    assert resolved == [expected for _, _, expected in oid_cases]


def test_sqlite_seven_field_description_reads_each_index(sqlite_memory):
    """Verify the 7-field DB-API description maps index by index.

    Mutation: an off-by-one on any description_item index in
        Column._extract_sqlite_column_info, e.g. nullable reading [5].
    Oracle: hand-written dict over a description whose seven fields all
        carry distinct values.
    """
    column = Column.from_cursor_description(
        ('quantity', 'integer(10)', 7, 8, 9, 10, 0), 'sqlite')

    assert column.to_dict() == {
        'name': 'quantity',
        'type_code': 'integer(10)',
        'python_type': 'int',
        'display_size': 7,
        'internal_size': 8,
        'precision': 9,
        'scale': 10,
        'nullable': False,
        }

    live = sqlite_memory.execute('select 1 as one').description[0]
    assert len(live) == 7


def test_sqlite_unknown_null_ok_stays_unknown(sqlite_memory):
    """Verify sqlite3's unknown null_ok stays None instead of becoming False.

    Mutation: bool(description_item[6]) in place of the None guard in
        Column._extract_sqlite_column_info, which turns "unknown" into a
        definite NOT NULL for every SQLite column.
    Oracle: the live sqlite3 description, which reports null_ok as None
        for the nullable column and the NOT NULL one alike.
    """
    sqlite_memory.execute(
        'create table trades (id integer not null, note text)')
    cursor = sqlite_memory.execute('select id, note from trades')

    assert [desc[6] for desc in cursor.description] == [None, None]

    columns = columns_from_cursor_description(cursor, 'sqlite')

    assert [col.nullable for col in columns] == [None, None]


def test_sqlite_reported_null_ok_narrows_to_a_bool():
    """Verify a driver that does report null_ok still yields True or False.

    Mutation: hardcoding nullable to None, or passing description_item[6]
        through unconverted, in Column._extract_sqlite_column_info.
    Oracle: DB-API null_ok flags 1 and 0, whose booleans differ by identity
        from None and from the raw ints they came from.
    """
    reported = [
        ('note', 'TEXT', None, None, None, None, 1),
        ('id', 'INTEGER', None, None, None, None, 0),
        ]

    columns = [
        Column.from_cursor_description(item, 'sqlite') for item in reported
        ]

    assert columns[0].nullable is True
    assert columns[1].nullable is False


def test_sqlite_short_description_drops_the_size_fields():
    """Verify a description shorter than 7 fields keeps only name and type.

    Mutation: relaxing `len(description_item) >= 7` to `>= 6` in
        Column._extract_sqlite_column_info.
    Oracle: a 6-field item straddling the threshold - its index 2 holds 7,
        yet display_size must stay None.
    """
    six = Column.from_cursor_description(
        ('quantity', 'integer(10)', 7, 8, 9, 10), 'sqlite')

    assert six.to_dict() == {
        'name': 'quantity',
        'type_code': 'integer(10)',
        'python_type': 'int',
        'display_size': None,
        'internal_size': None,
        'precision': None,
        'scale': None,
        'nullable': None,
        }

    two = Column.from_cursor_description(('note', 'TEXT'), 'sqlite')
    assert (two.name, two.type_code, two.nullable) == ('note', 'TEXT', None)


def test_sqlite_declared_type_normalized_before_lookup():
    """Verify a declared type is upper-cased and stripped of its size suffix.

    Mutation: dropping .upper() or the split('(')[0] from the sqlite branch
        of resolve_type's legacy lookup.
    Oracle: hand-computed 'integer(10)' -> int, 'blob' -> bytes; 'user_id'
        typed 'real' -> float beats the _id rule; 'balance' typed
        'numeric(10,2)' -> float requires both split('(')[0] and .upper().
    """
    declared_cases = [
        ('quantity', 'integer(10)', int),
        ('payload', 'blob', bytes),
        ('user_id', 'real', float),
        ('balance', 'numeric(10,2)', float),
        ]

    resolved = [
        Column.from_cursor_description((name, declared), 'sqlite').python_type
        for name, declared, _ in declared_cases
        ]

    assert resolved == [expected for _, _, expected in declared_cases]


def test_sqlite_untyped_columns_resolve_by_name_pattern(sqlite_memory):
    """Verify name patterns type the columns sqlite3 reports without a type.

    Mutation: dropping '_datetime' from the endswith tuple at types.py:375,
        so 'closing_datetime' falls through to str.
    Oracle: hand-computed type per name, over a real sqlite3 description
        whose type_code is always None.
    """
    sqlite_memory.execute(
        'create table trades (id, user_id, closing_datetime, signup_at, '
        'trade_date, start_time, is_active, unit_price, note)')
    cursor = sqlite_memory.execute('select * from trades')

    columns = columns_from_cursor_description(cursor, 'sqlite')

    assert [col.type_code for col in columns] == [None] * 9
    assert Column.get_types(columns) == [
        int,
        int,
        datetime.datetime,
        datetime.datetime,
        datetime.date,
        datetime.time,
        bool,
        float,
        str,
        ]


def test_unknown_dialect_keeps_only_a_stringified_name():
    """Verify an unrecognized dialect drops every field except the name.

    Mutation: dropping the str() around description_item[0], or reading
        type_code from the item, in Column.from_cursor_description's else
        branch.
    Oracle: an integer column name, which resolve_type's .lower() call
        would raise on if it arrived unstringified.
    """
    column = Column.from_cursor_description((7, 23, 5, 6, 7, 8, 1), 'mysql')

    assert column.to_dict() == {
        'name': '7',
        'type_code': None,
        'python_type': 'str',
        'display_size': None,
        'internal_size': None,
        'precision': None,
        'scale': None,
        'nullable': None,
        }

    empty = Column.from_cursor_description((), 'mysql')
    assert empty.name is None


def test_columns_from_cursor_description_preserves_column_order(sqlite_memory):
    """Verify one Column per description entry, in cursor order.

    Mutation: iterating reversed(cursor.description), or reusing
        cursor.description[0], in columns_from_cursor_description.
    Oracle: a select whose three columns resolve to three different types
        in a hand-written order.
    """
    sqlite_memory.execute('create table t (id, note, trade_date)')
    cursor = sqlite_memory.execute('select trade_date, id, note from t')

    columns = columns_from_cursor_description(cursor, 'sqlite')

    assert Column.get_names(columns) == ['trade_date', 'id', 'note']
    assert Column.get_types(columns) == [datetime.date, int, str]


def test_columns_from_cursor_description_empty_without_a_result_set(sqlite_memory):
    """Verify a statement with no result set yields no columns.

    Mutation: replacing the `cursor.description is None` guard with a
        falsy-or-empty test that lets None reach the comprehension.
    Oracle: a real sqlite3 DDL cursor, whose description the driver sets
        to None.
    """
    cursor = sqlite_memory.execute('create table t (id)')

    assert cursor.description is None
    assert columns_from_cursor_description(cursor, 'sqlite') == []


def test_dialect_name_labels_match_the_extraction_branches(
        create_simple_mock_connection, pg_desc):
    """Verify get_dialect_name() labels match from_cursor_description.

    Mutation: get_dialect_name returning 'postgres' for psycopg, or
        Column.from_cursor_description testing for 'sqlite3'.
    Oracle: metadata that only the matching branch can extract - index 2
        for sqlite, the display_size attribute for postgres.
    """
    pg_dialect = get_dialect_name(create_simple_mock_connection('postgresql'))
    sqlite_dialect = get_dialect_name(create_simple_mock_connection('sqlite'))

    assert (pg_dialect, sqlite_dialect) == ('postgresql', 'sqlite')

    from_sqlite = Column.from_cursor_description(
        ('quantity', 'integer', 7, 8, 9, 10, 0), sqlite_dialect)
    from_postgres = Column.from_cursor_description(
        pg_desc('amount', 1700, display_size=11), pg_dialect)

    assert from_sqlite.display_size == 7
    assert from_postgres.display_size == 11


def test_get_names_preserves_declaration_order():
    """Verify get_names() returns names in list order, not sorted.

    Mutation: sorting the result of Column.get_names.
    Oracle: hand-written list whose order differs from its sorted order.
    """
    columns = [
        Column(name='id', type_code=23, python_type=int),
        Column(name='name', type_code=25, python_type=str),
        Column(name='active', type_code=16, python_type=bool),
        ]

    assert Column.get_names(columns) == ['id', 'name', 'active']


def test_get_column_by_name_returns_the_first_match_or_none():
    """Verify get_column_by_name() matches on name and stops at the first hit.

    Mutation: flipping `col.name == name` to `!=`, or letting the loop run
        on and return the last match.
    Oracle: two columns sharing a name with different type codes, plus a
        name absent from the list.
    """
    first = Column(name='name', type_code=25, python_type=str)
    shadow = Column(name='name', type_code=1043, python_type=str)
    columns = [Column(name='id', type_code=23, python_type=int), first, shadow]

    assert Column.get_column_by_name(columns, 'name') is first
    assert Column.get_column_by_name(columns, 'nonexistent') is None


def test_get_types_returns_python_types_not_type_codes():
    """Verify get_types() reads python_type from each column.

    Mutation: returning col.type_code from Column.get_types.
    Oracle: hand-written type list against columns whose type codes are
        integers, never types.
    """
    columns = [
        Column(name='id', type_code=23, python_type=int),
        Column(name='note', type_code=25, python_type=str),
        Column(name='ratio', type_code=1700, python_type=float),
        ]

    assert Column.get_types(columns) == [int, str, float]


def test_get_column_types_dict_keys_by_name_with_full_metadata():
    """Verify get_column_types_dict() keys by column name and keeps every slot.

    Mutation: keying the dict by type_code, or storing the python_type
        object instead of its __name__ in Column.to_dict.
    Oracle: hand-written nested dict for a column with distinct metadata.
    """
    columns = [
        Column(
            name='id', type_code=23, python_type=int, display_size=11,
            internal_size=4, precision=None, scale=None, nullable=False),
        Column(
            name='ratio', type_code=1700, python_type=float,
            precision=10, scale=2, nullable=True),
        ]

    types_dict = Column.get_column_types_dict(columns)

    assert list(types_dict) == ['id', 'ratio']
    assert types_dict['id'] == {
        'name': 'id',
        'type_code': 23,
        'python_type': 'int',
        'display_size': 11,
        'internal_size': 4,
        'precision': None,
        'scale': None,
        'nullable': False,
        }
    assert types_dict['ratio']['python_type'] == 'float'
    assert (types_dict['ratio']['precision'], types_dict['ratio']['scale']) == (10, 2)


def test_create_empty_columns_leaves_every_slot_unset():
    """Verify create_empty_columns() sets only the name.

    Mutation: create_empty_columns defaulting python_type to str, or
        to_dict dropping its None guard on python_type.__name__.
    Oracle: hand-written dict of Nones per name, in the given order.
    """
    columns = Column.create_empty_columns(['id', 'name', 'active'])

    assert [col.name for col in columns] == ['id', 'name', 'active']
    assert columns[0].to_dict() == {
        'name': 'id',
        'type_code': None,
        'python_type': None,
        'display_size': None,
        'internal_size': None,
        'precision': None,
        'scale': None,
        'nullable': None,
        }


def test_repr_names_the_python_type_and_quotes_the_column():
    """Verify Column.__repr__ prints the type name, not the type object.

    Mutation: dropping .__name__ from python_type, or !r from name, in
        Column.__repr__.
    Oracle: hand-written expected string.
    """
    column = Column(name='ratio', type_code=1700, python_type=float)
    empty = Column(name='ratio', type_code=None)

    assert repr(column) == "Column(name='ratio', type_code=1700, python_type=float)"
    assert repr(empty) == "Column(name='ratio', type_code=None, python_type=None)"


if __name__ == '__main__':
    __import__('pytest').main([__file__])
