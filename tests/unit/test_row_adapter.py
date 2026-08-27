"""Tests for the psycopg row factory and the structural row adapter.

Covers DictRowFactory (database.row), which casts numeric column values
by PostgreSQL type OID, and RowAdapter (database.types), which reshapes
driver rows into dictionaries without converting any value.
"""
import datetime
import math
import sqlite3
from collections import namedtuple
from decimal import Decimal

import pytest
from database.row import DictRowFactory
from database.strategy import get_strategy
from database.types import RowAdapter

# Notes:
# - OIDs are written out from the pg_catalog.pg_type catalog rather
#   than read back through psycopg, so the expected mapping is an
#   oracle independent of _build_postgres_types().
# - OID_OID (the pg_catalog 'oid' type) carries an int and has no
#   entry in postgres_types, which is what makes it the uncast case.
OID_BOOL = 16
OID_INT8 = 20
OID_TEXT = 25
OID_OID = 26
OID_FLOAT8 = 701
OID_INT4_ARRAY = 1007
OID_DATE = 1082
OID_NUMERIC = 1700
OID_JSONB = 3802

FakeColumn = namedtuple('FakeColumn', ['name', 'type_code'])


class FakeCursor:
    """Stand-in for a psycopg cursor exposing only description.
    """

    def __init__(self, description):
        self.description = description


def test_dict_row_factory_casts_numeric_values_by_type_code():
    """Verify each numeric value is cast to the type its OID maps to.

    Mutation: dropping 'numeric' from the float row of type_mappings in
    _build_postgres_types, or mapping bool to int there.
    Oracle: hand-written 123.45, which Decimal('123.45') compares
    unequal to, and True, which int 1 is not identical to.
    """
    factory = DictRowFactory(FakeCursor([
        FakeColumn('id', OID_INT8),
        FakeColumn('amount', OID_NUMERIC),
        FakeColumn('ratio', OID_FLOAT8),
        FakeColumn('flag', OID_BOOL),
        ]))

    result = factory((42, Decimal('123.45'), 0.5, True))

    assert result == {'id': 42, 'amount': 123.45, 'ratio': 0.5, 'flag': True}
    assert list(result) == ['id', 'amount', 'ratio', 'flag']
    assert type(result['amount']) is float
    assert result['flag'] is True


def test_dict_row_factory_leaves_non_numeric_values_untouched():
    """Verify a cast fires for Number values only, never for other types.

    Mutation: dropping `isinstance(value, Number) and` from the
    comprehension in DictRowFactory.__call__.
    Oracle: identity of the input objects; datetime.date(a_date) raises
    TypeError, and tuple([1, 2, 3]) compares unequal to the input list.
    """
    when = datetime.date(2023, 5, 15)
    payload = {'a': 1}
    ids = [1, 2, 3]
    factory = DictRowFactory(FakeCursor([
        FakeColumn('note', OID_TEXT),
        FakeColumn('when', OID_DATE),
        FakeColumn('payload', OID_JSONB),
        FakeColumn('ids', OID_INT4_ARRAY),
        ]))

    result = factory(('null', when, payload, ids))

    assert result['note'] == 'null'
    assert result['when'] is when
    assert result['payload'] is payload
    assert result['ids'] is ids


def test_dict_row_factory_passes_through_unmapped_type_code():
    """Verify a column whose OID has no mapping is returned uncast.

    Mutation: `postgres_types.get(c.type_code, str)` in
    DictRowFactory.__init__, or dropping `and cast is not None` from
    __call__.
    Oracle: OID 26 is absent from postgres_types, so str(12345) is the
    only other value the row could carry and None(12345) raises.
    """
    factory = DictRowFactory(FakeCursor([FakeColumn('obj_id', OID_OID)]))

    result = factory((12345,))

    assert result == {'obj_id': 12345}
    assert type(result['obj_id']) is int


def test_dict_row_factory_preserves_sql_null():
    """Verify NULL in a cast column stays None rather than becoming 0.0.

    Mutation: dropping `isinstance(value, Number) and` from
    DictRowFactory.__call__, which evaluates float(None) and int(None).
    Oracle: None is not a Number, so None is the only correct output.
    """
    factory = DictRowFactory(FakeCursor([
        FakeColumn('amount', OID_NUMERIC),
        FakeColumn('id', OID_INT8),
        ]))

    result = factory((None, None))

    assert result == {'amount': None, 'id': None}


def test_dict_row_factory_handles_cursor_without_description():
    """Verify a cursor with no result columns yields an empty mapping.

    Mutation: dropping `or []` from DictRowFactory.__init__, which makes
    construction raise TypeError for a statement returning no rows.
    Oracle: psycopg leaves description None after such a statement, so
    {} is the only non-raising result.
    """
    factory = DictRowFactory(FakeCursor(None))

    assert factory(()) == {}


def test_postgres_dict_cursor_uses_dict_row_factory():
    """Verify the postgres strategy wires DictRowFactory into the cursor.

    Mutation: create_dict_cursor passing psycopg's dict_row, or omitting
    row_factory, which silently drops the numeric casting above.
    Oracle: a spy connection recording the kwargs the strategy passed.
    """
    class SpyConnection:
        """Records the keyword arguments passed to cursor().
        """

        def __init__(self):
            self.cursor_kwargs = None

        def cursor(self, **kwargs):
            self.cursor_kwargs = kwargs
            return 'cursor-sentinel'

    connection = SpyConnection()

    cursor = get_strategy('postgresql').create_dict_cursor(connection)

    assert cursor == 'cursor-sentinel'
    assert connection.cursor_kwargs == {'row_factory': DictRowFactory}


def test_sqlite_dict_cursor_returns_rows_keyed_by_column():
    """Verify the sqlite strategy sets sqlite3.Row on the raw connection.

    Mutation: dropping `sqlite_conn.row_factory = sqlite3.Row` from
    SQLiteStrategy.create_dict_cursor, or dropping the dbapi_connection
    unwrap above it.
    Oracle: hand-written {'a': 1, 'b': 'x'}, which the plain tuple
    (1, 'x') a factory-less cursor returns compares unequal to.
    """
    class Wrapper:
        def __init__(self, connection):
            self.dbapi_connection = connection

    raw = sqlite3.connect(':memory:')
    cursor = get_strategy('sqlite').create_dict_cursor(Wrapper(raw))
    row = cursor.execute("select 1 as a, 'x' as b").fetchone()
    raw.close()

    assert RowAdapter(row).to_dict() == {'a': 1, 'b': 'x'}


def test_row_adapter_dict_row_applies_no_type_conversion():
    """Verify a dict row is handed back with every value untouched.

    Mutation: routing values through TypeConverter.convert_value in
    RowAdapter.to_dict or get_value.
    Oracle: identity of the input objects, plus the string 'null' and a
    NaN float, which TypeConverter turns into None.
    """
    total = Decimal('123.45')
    when = datetime.date(2023, 5, 15)
    row = {'total': total, 'note': 'null', 'ratio': float('nan'), 'when': when}

    adapter = RowAdapter(row)
    result = adapter.to_dict()

    assert list(result) == ['total', 'note', 'ratio', 'when']
    assert result['total'] is total
    assert result['note'] == 'null'
    assert math.isnan(result['ratio'])
    assert result['when'] is when
    assert adapter.get_value('total') is total
    assert adapter.get_value('note') == 'null'


def test_row_adapter_sqlite_row_converts_to_plain_dict():
    """Verify a sqlite3.Row becomes a dict keyed by column name.

    Mutation: dropping the keys() branch from RowAdapter.to_dict, which
    returns the sqlite3.Row itself, or reading keys[-1] instead of
    keys[0] in get_value().
    Oracle: hand-written expected dict, which a sqlite3.Row compares
    unequal to; the first column 123 differs from the last 45.6.
    """
    connection = sqlite3.connect(':memory:')
    connection.row_factory = sqlite3.Row
    row = connection.execute(
        "select 123 as int_col, 'null' as text_col, 45.6 as real_col").fetchone()
    connection.close()

    adapter = RowAdapter(row)

    assert adapter.to_dict() == {
        'int_col': 123,
        'text_col': 'null',
        'real_col': 45.6,
        }
    assert adapter.get_value('text_col') == 'null'
    assert adapter.get_value() == 123


def test_row_adapter_namedtuple_row():
    """Verify a namedtuple row expands by field name and reads by key.

    Mutation: dropping the _asdict branch from RowAdapter.to_dict, or the
    hasattr(self.row, key) branch from get_value, which sends a string
    key into tuple.__getitem__.
    Oracle: hand-written expected dict, which a namedtuple compares
    unequal to; field 0 (7) differs from field 1 ('bob').
    """
    Record = namedtuple('Record', ['id', 'name'])
    row = Record(7, 'bob')

    adapter = RowAdapter(row)

    assert adapter.to_dict() == {'id': 7, 'name': 'bob'}
    assert adapter.get_value('name') == 'bob'
    assert adapter.get_value() == 7


def test_row_adapter_to_attrdict_allows_attribute_access():
    """Verify select_row's row supports both row.column and row['column'].

    Mutation: RowAdapter.to_attrdict returning self.to_dict() instead of
    wrapping it in attrdict.
    Oracle: an attribute read, which raises AttributeError on a plain
    dict, and the stored Decimal reached by key.
    """
    total = Decimal('1.50')
    row = {'name': 'alice', 'total': total}

    result = RowAdapter(row).to_attrdict()

    assert result.name == 'alice'
    assert result['total'] is total


def test_row_adapter_get_value_raises_on_missing_key():
    """Verify an absent column raises instead of yielding None.

    Mutation: RowAdapter.get_value using self.row.get(key), which turns a
    misspelled column into a silent None.
    Oracle: KeyError on a key the row does not carry.
    """
    adapter = RowAdapter({'id': 1})

    with pytest.raises(KeyError):
        adapter.get_value('nope')


def test_row_adapter_create_ignores_connection(create_simple_mock_connection):
    """Verify create() wraps the row unchanged whatever the dialect is.

    Mutation: RowAdapter.create dispatching on dialect, or re-wrapping
    the row as RowAdapter(TypeConverter.convert_params(row)).
    Oracle: the same row object comes back for both dialects, and the
    string 'nan' survives, which TypeConverter would null out.
    """
    row = {'note': 'nan', 'total': Decimal('123.45')}

    pg_adapter = RowAdapter.create(create_simple_mock_connection('postgresql'), row)
    lite_adapter = RowAdapter.create(create_simple_mock_connection('sqlite'), row)

    assert pg_adapter.row is row
    assert lite_adapter.row is row
    assert pg_adapter.to_dict() == {'note': 'nan', 'total': Decimal('123.45')}
    assert lite_adapter.to_dict() == pg_adapter.to_dict()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
