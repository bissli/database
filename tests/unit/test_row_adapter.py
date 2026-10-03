"""Tests for DictRowFactory and RowAdapter."""
import datetime
import math
import sqlite3
from collections import namedtuple
from decimal import Decimal

import pytest
from database.row import DictRowFactory
from database.strategy import get_strategy
from database.types import RowAdapter

# Literal pg_type OIDs keep the oracle independent of
# _build_postgres_types.
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

    Mutation: 'numeric' dropped from the float row, or bool mapped to int.
    Oracle: float 123.45 by exact type, and True by identity.
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

    Mutation: dropping `isinstance(value, Number) and` from __call__.
    Oracle: identity of the date, dict, and list input objects.
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

    Mutation: a str default in postgres_types.get, or no None check.
    Oracle: OID 26 is absent from postgres_types, so 12345 stays an int.
    """
    factory = DictRowFactory(FakeCursor([FakeColumn('obj_id', OID_OID)]))

    result = factory((12345,))

    assert result == {'obj_id': 12345}
    assert type(result['obj_id']) is int


def test_dict_row_factory_preserves_sql_null():
    """Verify a null in a cast column stays None rather than becoming 0.0.

    Mutation: dropping the Number check, which calls float(None).
    Oracle: None is not a Number, so None must come back.
    """
    factory = DictRowFactory(FakeCursor([
        FakeColumn('amount', OID_NUMERIC),
        FakeColumn('id', OID_INT8),
        ]))

    result = factory((None, None))

    assert result == {'amount': None, 'id': None}


def test_dict_row_factory_handles_cursor_without_description():
    """Verify a cursor with no result columns yields an empty mapping.

    Mutation: dropping `or []` from DictRowFactory.__init__.
    Oracle: psycopg's None description after a rowless statement.
    """
    factory = DictRowFactory(FakeCursor(None))

    assert factory(()) == {}


def test_postgres_dict_cursor_uses_dict_row_factory():
    """Verify the postgres strategy wires DictRowFactory into the cursor.

    Mutation: create_dict_cursor passing dict_row or no row_factory.
    Oracle: a spy connection recording the cursor kwargs.
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

    Mutation: dropping the sqlite3.Row assignment or the dbapi unwrap.
    Oracle: {'a': 1, 'b': 'x'}, which a plain tuple row does not equal.
    """
    class Wrapper:
        """Exposes a raw connection as dbapi_connection.
        """

        def __init__(self, connection):
            self.dbapi_connection = connection

    raw = sqlite3.connect(':memory:')
    cursor = get_strategy('sqlite').create_dict_cursor(Wrapper(raw))
    row = cursor.execute("select 1 as a, 'x' as b").fetchone()
    raw.close()

    assert RowAdapter(row).to_dict() == {'a': 1, 'b': 'x'}


def test_row_adapter_dict_row_applies_no_type_conversion():
    """Verify a dict row is handed back with every value untouched.

    Mutation: routing values through TypeConverter.convert_value.
    Oracle: input identity, 'null', and NaN, which TypeConverter nulls.
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

    Mutation: dropping the keys() branch, or keys[-1] for keys[0].
    Oracle: hand-written dict; the first column differs from the last.
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

    Mutation: dropping the _asdict branch or the hasattr(row, key) read.
    Oracle: hand-written dict; field 0 differs from field 1.
    """
    Record = namedtuple('Record', ['id', 'name'])
    row = Record(7, 'bob')

    adapter = RowAdapter(row)

    assert adapter.to_dict() == {'id': 7, 'name': 'bob'}
    assert adapter.get_value('name') == 'bob'
    assert adapter.get_value() == 7


def test_row_adapter_to_attrdict_allows_attribute_access():
    """Verify select_row's row supports both row.column and row['column'].

    Mutation: to_attrdict returning a plain dict.
    Oracle: an attribute read, which a plain dict refuses.
    """
    total = Decimal('1.50')
    row = {'name': 'alice', 'total': total}

    result = RowAdapter(row).to_attrdict()

    assert result.name == 'alice'
    assert result['total'] is total


def test_row_adapter_get_value_raises_on_missing_key():
    """Verify an absent column raises instead of yielding None.

    Mutation: self.row.get(key), which returns None for a typo.
    Oracle: KeyError on a key the row does not carry.
    """
    adapter = RowAdapter({'id': 1})

    with pytest.raises(KeyError):
        adapter.get_value('nope')


def test_row_adapter_create_ignores_connection(create_simple_mock_connection):
    """Verify create() wraps the row unchanged whatever the dialect is.

    Mutation: create dispatching on dialect or converting the row.
    Oracle: the same row object for both dialects, 'nan' intact.
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
