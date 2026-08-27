"""
Tests for adapter/converter registration and the Single Conversion Principle.

Inbound conversion happens once, in TypeConverter during parameter
binding. Outbound conversion happens once, in the driver converters
registered for the connection.
"""
import datetime
import math
import sqlite3

import database as db
import database.strategy.sqlite as sqlite_strategy
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from database.types import TypeConverter, convert_date, convert_datetime
from database.types import get_adapter_registry

CREATE_PROBE_TABLE = """
CREATE TABLE conv_probe (
    label TEXT,
    d DATE,
    ts DATETIME,
    txt TEXT
)
"""


@pytest.fixture
def sqlite_conn():
    """In-memory SQLite connection holding a table of declared types."""
    cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    db.execute(cn, CREATE_PROBE_TABLE)
    yield cn
    cn.close()


def test_adapter_registry_registers_both_sqlite_converters(monkeypatch):
    """Verify AdapterRegistry.sqlite() binds each decltype to its converter.

    Mutation: registering convert_date under 'datetime' in
        AdapterRegistry.sqlite, or dropping either register_converter
        call.
    Oracle: hand-written pair list [('date', convert_date),
        ('datetime', convert_datetime)] compared by identity.
    """
    registered = []
    monkeypatch.setattr(
        sqlite3,
        'register_converter',
        lambda name, converter: registered.append((name, converter)))

    class ProbeConnection:
        def __init__(self):
            self.executed = []

        def execute(self, sql):
            self.executed.append(sql)

    connection = ProbeConnection()
    get_adapter_registry().sqlite(connection)

    assert registered == [
        ('date', convert_date),
        ('datetime', convert_datetime),
        ]
    assert connection.executed == ['SELECT 1']


def test_convert_date_drops_the_time_component():
    """Verify convert_date() returns a date, not the parsed datetime.

    Mutation: dropping the trailing .date() call in convert_date.
    Oracle: hand-computed date(2023, 5, 15) plus an exact type check,
        since datetime is a subclass of date and passes isinstance.
    """
    result = convert_date(b'2023-05-15 14:30:45')

    assert result == datetime.date(2023, 5, 15)
    assert type(result) is datetime.date


def test_convert_datetime_keeps_offset_and_microseconds():
    """Verify convert_datetime() parses full ISO 8601, not a fixed format.

    Mutation: strptime('%Y-%m-%d %H:%M:%S') in place of isoparse in
        convert_datetime, which loses microseconds and the offset.
    Oracle: hand-computed datetimes, one with a +02:00 tzinfo built
        from datetime.timezone.
    """
    offset = datetime.timezone(datetime.timedelta(hours=2))

    assert convert_datetime(b'2023-05-15T14:30:45.123456+02:00') == \
        datetime.datetime(2023, 5, 15, 14, 30, 45, 123456, tzinfo=offset)
    assert convert_datetime(b'2023-05-15 14:30:45') == \
        datetime.datetime(2023, 5, 15, 14, 30, 45)
    assert convert_datetime(b'2023-05-15T14:30:45.123456').microsecond == 123456


def test_sqlite_date_converter_runs_once_per_fetched_value():
    """Verify a DATE column converts once outbound and never inbound.

    Mutation: dropping detect_types from SQLiteStrategy.get_engine_kwargs,
        which leaves the converter unregistered and hands back a string.
    Oracle: a counting spy wrapping convert_date - zero calls after the
        insert, exactly one after fetching one row.
    """
    calls = []

    def counting_convert_date(raw):
        calls.append(raw)
        return convert_date(raw)

    original = sqlite_strategy.convert_date
    sqlite_strategy.convert_date = counting_convert_date
    try:
        cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
        db.execute(cn, CREATE_PROBE_TABLE)
        db.execute(
            cn,
            'INSERT INTO conv_probe (d) VALUES (?)',
            datetime.date(2023, 5, 15))

        assert calls == []

        value = db.select_scalar(cn, 'SELECT d FROM conv_probe')
        cn.close()
    finally:
        sqlite_strategy.convert_date = original
        sqlite3.register_converter('date', convert_date)

    assert calls == [b'2023-05-15']
    assert value == datetime.date(2023, 5, 15)
    assert type(value) is datetime.date


def test_sqlite_bind_converts_special_string_to_null(sqlite_conn):
    """Verify inbound conversion runs at bind time, on special strings only.

    Mutation: dropping the TypeConverter.convert_params call in
        Cursor.execute, which stores the literal text 'null'.
    Oracle: two rows differing only in the bound value - 'null' lands
        as SQL NULL, '0' lands unchanged.
    """
    db.execute(
        sqlite_conn,
        'INSERT INTO conv_probe (label, txt) VALUES (?, ?)',
        'special',
        'null')
    db.execute(
        sqlite_conn,
        'INSERT INTO conv_probe (label, txt) VALUES (?, ?)',
        'plain',
        '0')

    assert db.select_column(
        sqlite_conn,
        'SELECT txt FROM conv_probe ORDER BY label') == ['0', None]


def test_type_converter_special_strings():
    """Verify only the exact null-ish words, case-folded, become NULL.

    Mutation: dropping .lower() or the value == '' arm in
        _check_special_string, or matching by prefix instead of set
        membership.
    Oracle: hand-picked boundary strings straddling the set - 'nap',
        'nan ' and ' ' sit just outside it.
    """
    for special in ('', 'null', 'NULL', 'nan', 'NaN', 'none', 'None',
                    'na', 'NA', 'nat', 'NaT'):
        assert TypeConverter.convert_value(special) is None, special

    for kept in ('nap', 'nan ', ' ', '0', 'false', 'n/a', 'nulls', 'hello'):
        assert TypeConverter.convert_value(kept) == kept


def test_type_converter_numpy_scalars_unbox_to_builtins():
    """Verify NumPy scalars come back as exact builtins, not NumPy types.

    Mutation: returning val instead of val.item() in
        _convert_numpy_value - equality still holds, the type does not.
    Oracle: exact type identity plus hand-computed values.
    """
    as_int = TypeConverter.convert_value(np.int64(42))
    assert as_int == 42
    assert type(as_int) is int
    assert type(TypeConverter.convert_value(np.int32(-7))) is int
    assert type(TypeConverter.convert_value(np.uint64(42))) is int

    as_float = TypeConverter.convert_value(np.float64(math.pi))
    assert as_float == math.pi
    assert type(as_float) is float
    assert abs(TypeConverter.convert_value(np.float32(math.pi)) - math.pi) < 1e-6

    assert TypeConverter.convert_value(np.float64('nan')) is None
    assert TypeConverter.convert_value(np.float32('nan')) is None


def test_type_converter_non_finite_floats_become_null():
    """Verify infinities are nulled on the builtin-float fast path, like NaN.

    Mutation: dropping `or math.isinf(value)` from the value_type is
        float branch of convert_value, which returns inf unchanged.
    Oracle: float('inf') and float('-inf'), values no database column
        accepts, against a finite control.
    """
    assert TypeConverter.convert_value(float('inf')) is None
    assert TypeConverter.convert_value(float('-inf')) is None
    assert TypeConverter.convert_value(float('nan')) is None
    assert TypeConverter.convert_value(np.float64('inf')) is None
    assert TypeConverter.convert_value(0.0) == 0.0


def test_type_converter_datetime64_truncates_to_seconds():
    """Verify datetime64 converts through a seconds-resolution epoch.

    Mutation: astype('datetime64[ms]') in place of
        astype('datetime64[s]') in _convert_numpy_value.
    Oracle: hand-computed datetime(2023, 1, 15, 12, 34, 56) from an
        input carrying 789 milliseconds.
    """
    result = TypeConverter.convert_value(np.datetime64('2023-01-15T12:34:56.789'))

    assert result == datetime.datetime(2023, 1, 15, 12, 34, 56)
    assert type(result) is datetime.datetime
    assert TypeConverter.convert_value(np.datetime64('NaT')) is None


def test_type_converter_missing_scalars_become_null():
    """Verify every missing-value marker binds as NULL.

    Mutation: replacing the exact `value_type is datetime.datetime`
        check in convert_value with isinstance, which short-circuits
        pd.NaT (a datetime subclass) straight back to the caller.
    Oracle: pd.NaT, pd.NA and a pandas Int64 missing element, each
        against the None the driver needs.
    """
    assert TypeConverter.convert_value(pd.NaT) is None
    assert TypeConverter.convert_value(pd.NA) is None
    assert TypeConverter.convert_value(None) is None

    nullable_ds = pd.Series([1, 2, None], dtype='Int64')
    assert TypeConverter.convert_value(nullable_ds[2]) is None
    assert type(TypeConverter.convert_value(nullable_ds[0])) is int


def test_convert_value_leaves_driver_owned_values_alone():
    """Verify the is_scalar guard fires before pd.isna on non-scalar input.

    Mutation: dropping pd.api.types.is_scalar(value) before pd.isna(value)
        in convert_value - pd.isna on a list returns an array and raises.
    Oracle: list and tuple containers; without the guard the call raises
        instead of returning the container.
    """
    assert TypeConverter.convert_value([1, 2, 3]) == [1, 2, 3]
    assert TypeConverter.convert_value((1, 2)) == (1, 2)


def test_type_converter_dict_params():
    """Verify dict params convert values and leave keys alone.

    Mutation: transposing the dict comprehension in convert_params to
        convert keys instead of values.
    Oracle: a key that is itself a special string ('null') paired with
        a value that must convert, so a transposition swaps which side
        goes None.
    """
    params = {
        'null': np.float64(42.5),
        'name': 'test',
        'empty': '',
        'nan_val': float('nan'),
        }
    converted = TypeConverter.convert_params(params)

    assert converted == {
        'null': 42.5,
        'name': 'test',
        'empty': None,
        'nan_val': None,
        }
    assert type(converted['null']) is float


def test_convert_params_preserves_container_shape():
    """Verify convert_params keeps the container type and recurses one level.

    Mutation: returning list(...) instead of type(params)(...) in
        convert_params, or dropping the all-sequences branch that
        recurses into a batch of rows.
    Oracle: hand-written expected rows plus exact container and element
        types, which equality alone cannot tell apart (np.int64(1) == 1).
    """
    single = TypeConverter.convert_params((np.int64(1), 'null'))
    assert single == (1, None)
    assert type(single) is tuple
    assert type(single[0]) is int

    batch = TypeConverter.convert_params([
        (np.int64(1), 'nan'),
        ('x', np.float64('nan')),
        ])
    assert batch == [(1, None), ('x', None)]
    assert type(batch) is list
    assert type(batch[0]) is tuple
    assert type(batch[0][0]) is int


def test_pyarrow_values_unbox_and_null_special_strings():
    """Verify PyArrow scalars unbox to builtins and honor the null-string rule.

    Mutation: returning value.as_py() unwrapped in
        _convert_pyarrow_value, so an Arrow string scalar holding
        'null' binds as text.
    Oracle: hand-written expectations - 'null' -> None, 3.5 -> 3.5,
        and an Arrow array -> the plain list [1, 2, 3].
    """
    assert TypeConverter.convert_value(pa.scalar('null')) is None
    assert TypeConverter.convert_value(pa.scalar('hello')) == 'hello'

    as_float = TypeConverter.convert_value(pa.scalar(3.5))
    assert as_float == 3.5
    assert type(as_float) is float

    as_list = TypeConverter.convert_value(pa.array([1, 2, 3]))
    assert as_list == [1, 2, 3]
    assert type(as_list) is list
    assert type(as_list[0]) is int


def test_datetime_converter_round_trip(sqlite_conn):
    """Verify a DATETIME column round-trips as datetime.datetime.

    Mutation: register_converter('datetime', convert_date) at
        strategy/sqlite.py:79 - a date converter drops the time part.
    Oracle: hand-written datetime with microseconds, which a date
        converter cannot carry.
    """
    db.execute(
        sqlite_conn,
        'INSERT INTO conv_probe (label, ts) VALUES (?, ?)',
        'dt',
        datetime.datetime(2023, 5, 15, 14, 30, 45, 123456))

    value = db.select_scalar(
        sqlite_conn,
        'SELECT ts FROM conv_probe WHERE label = ?',
        'dt')

    assert value == datetime.datetime(2023, 5, 15, 14, 30, 45, 123456)
    assert type(value) is datetime.datetime


def test_sqlite_executemany_converts_special_strings(sqlite_conn):
    """Verify batch insert converts special strings to NULL.

    Mutation: dropping TypeConverter.convert_params in
        Cursor.executemany (cursor.py:266), which stores 'null' as text.
    Oracle: two rows differing only in bound value - 'null' lands
        as SQL NULL, '0' lands unchanged.
    """
    db.insert_rows(
        sqlite_conn,
        'conv_probe',
        [{'label': 'a', 'txt': 'null'}, {'label': 'b', 'txt': '0'}])

    assert db.select_column(
        sqlite_conn,
        'SELECT txt FROM conv_probe ORDER BY label') == [None, '0']


def test_sqlite_list_adapter_binds_as_json(sqlite_conn):
    """Verify a list value is stored as JSON text via the registered adapter.

    Mutation: dropping sqlite3.register_adapter(list, json.dumps) from
        SQLiteStrategy.register_type_adapters - without it sqlite3
        raises InterfaceError on the bind.
    Oracle: hand-written '[1, 2, 3]' - without the adapter the call
        raises instead of returning the JSON text.
    """
    db.execute(
        sqlite_conn,
        'INSERT INTO conv_probe (label, txt) VALUES (?, ?)',
        'listval',
        [1, 2, 3])

    result = db.select_scalar(
        sqlite_conn,
        'SELECT txt FROM conv_probe WHERE label = ?',
        'listval')

    assert result == '[1, 2, 3]'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
