"""Tests for Column metadata built from cursor descriptions."""
import datetime
import sqlite3
from types import SimpleNamespace

import pytest
from database.types import Column, columns_from_cursor_description
from database.utils import get_dialect_name


@pytest.fixture
def pg_desc():
    """Factory for attribute-only psycopg-style description items."""
    def factory(
        name, type_code, display_size=None, internal_size=None,
        precision=None, scale=None):
        """Description item with the given fields."""
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

    Mutation: swapping two size slots in _extract_postgres_column_info.
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

    Mutation: the name-pattern block moved above the type-code lookup.
    Oracle: catalog OIDs 1082 (date) and 1114 (timestamp).
    """
    dated = Column.from_cursor_description(
        pg_desc('created_at', 1082), 'postgresql')
    stamped = Column.from_cursor_description(
        pg_desc('start_time', 1114), 'postgresql')

    assert dated.python_type is datetime.date
    assert stamped.python_type is datetime.datetime


def test_sqlite_seven_field_description_reads_each_index(sqlite_memory):
    """Verify the 7-field DB-API description maps index by index.

    Mutation: an off-by-one index in _extract_sqlite_column_info.
    Oracle: hand-written dict over seven distinct field values.
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

    Mutation: bool(description_item[6]) without the None guard.
    Oracle: live sqlite3, which reports null_ok None for every column.
    """
    sqlite_memory.execute(
        'create table trades (id integer not null, note text)')
    cursor = sqlite_memory.execute('select id, note from trades')

    assert [desc[6] for desc in cursor.description] == [None, None]

    columns = columns_from_cursor_description(cursor, 'sqlite')

    assert [col.nullable for col in columns] == [None, None]


def test_sqlite_reported_null_ok_narrows_to_a_bool():
    """Verify a driver that does report null_ok still yields True or False.

    Mutation: nullable hardcoded None, or [6] passed through raw.
    Oracle: null_ok 1 and 0, checked by identity against True and False.
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

    Mutation: `>= 6` in place of `>= 7` on the description length.
    Oracle: a 6-field item whose index 2 holds 7; display_size stays None.
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


def test_sqlite_untyped_columns_resolve_by_name_pattern(sqlite_memory):
    """Verify name patterns type the columns sqlite3 reports without a type.

    Mutation: dropping '_datetime' from the datetime suffix tuple.
    Oracle: hand-computed type per name over a live sqlite3 description.
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

    Mutation: dropping str() on the name, or reading type_code, in the
        else branch of from_cursor_description.
    Oracle: an int column name, on which .lower() would raise.
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

    Mutation: iterating reversed(cursor.description).
    Oracle: three columns of three types in a hand-written order.
    """
    sqlite_memory.execute('create table t (id, note, trade_date)')
    cursor = sqlite_memory.execute('select trade_date, id, note from t')

    columns = columns_from_cursor_description(cursor, 'sqlite')

    assert Column.get_names(columns) == ['trade_date', 'id', 'note']
    assert Column.get_types(columns) == [datetime.date, int, str]


def test_columns_from_cursor_description_empty_without_a_result_set(sqlite_memory):
    """Verify a statement with no result set yields no columns.

    Mutation: dropping the `cursor.description is None` guard.
    Oracle: a live sqlite3 DDL cursor, whose description is None.
    """
    cursor = sqlite_memory.execute('create table t (id)')

    assert cursor.description is None
    assert columns_from_cursor_description(cursor, 'sqlite') == []


def test_dialect_name_labels_match_the_extraction_branches(
        create_simple_mock_connection, pg_desc):
    """Verify get_dialect_name() labels match from_cursor_description.

    Mutation: get_dialect_name returning 'postgres', or a 'sqlite3' branch.
    Oracle: fields only the matching extraction branch can read.
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
    Oracle: a hand-written list that is not in sorted order.
    """
    columns = [
        Column(name='id', type_code=23, python_type=int),
        Column(name='name', type_code=25, python_type=str),
        Column(name='active', type_code=16, python_type=bool),
        ]

    assert Column.get_names(columns) == ['id', 'name', 'active']


def test_get_column_by_name_returns_the_first_match_or_none():
    """Verify get_column_by_name() matches on name and stops at the first hit.

    Mutation: `!=` for `==`, or returning the last match.
    Oracle: two columns sharing a name, and an absent name.
    """
    first = Column(name='name', type_code=25, python_type=str)
    shadow = Column(name='name', type_code=1043, python_type=str)
    columns = [Column(name='id', type_code=23, python_type=int), first, shadow]

    assert Column.get_column_by_name(columns, 'name') is first
    assert Column.get_column_by_name(columns, 'nonexistent') is None


def test_get_types_returns_python_types_not_type_codes():
    """Verify get_types() reads python_type from each column.

    Mutation: returning col.type_code from Column.get_types.
    Oracle: hand-written types against integer type codes.
    """
    columns = [
        Column(name='id', type_code=23, python_type=int),
        Column(name='note', type_code=25, python_type=str),
        Column(name='ratio', type_code=1700, python_type=float),
        ]

    assert Column.get_types(columns) == [int, str, float]


def test_get_column_types_dict_keys_by_name_with_full_metadata():
    """Verify get_column_types_dict() keys by column name and keeps every slot.

    Mutation: keying by type_code, or storing the type for its __name__.
    Oracle: hand-written nested dict with distinct metadata.
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

    Mutation: python_type defaulting to str, or no None guard in to_dict.
    Oracle: hand-written dict of Nones, names in the given order.
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

    Mutation: dropping .__name__ or !r in Column.__repr__.
    Oracle: hand-written expected string.
    """
    column = Column(name='ratio', type_code=1700, python_type=float)
    empty = Column(name='ratio', type_code=None)

    assert repr(column) == "Column(name='ratio', type_code=1700, python_type=float)"
    assert repr(empty) == "Column(name='ratio', type_code=None, python_type=None)"


if __name__ == '__main__':
    __import__('pytest').main([__file__])
